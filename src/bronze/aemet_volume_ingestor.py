import json
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.secrets import get_secret  # noqa: E402
from src.catalog import BRONZE_AEMET_PATH  # noqa: E402

DEFAULT_BASE_URL = "https://opendata.aemet.es/opendata/api"
DEFAULT_PROVINCES: Set[str] = {"BADAJOZ", "CACERES"}
DEFAULT_OUTPUT_FILE = BRONZE_AEMET_PATH


def _get_api_key(api_key: Optional[str] = None) -> str:
    key = (api_key or get_secret("AEMET_API_KEY")).strip()
    if not key:
        raise ValueError("No se encontró AEMET_API_KEY en Databricks Secrets.")
    return key


def _parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def _format_aemet_datetime(d: date, end_of_day: bool = False) -> str:
    suffix = "T23:59:59UTC" if end_of_day else "T00:00:00UTC"
    return f"{d.isoformat()}{suffix}"


def _merge_tmp_into_volume(tmp_jsonl: Path, volume_json: Path, overwrite_volume: bool = False) -> None:
    # Fusiona /tmp -> Volume sin append directo sobre FUSE.
    volume_json.parent.mkdir(parents=True, exist_ok=True)
    merged_tmp = Path(f"/tmp/merged_{volume_json.name}")

    with merged_tmp.open("wb") as out:
        if not overwrite_volume and volume_json.exists():
            with volume_json.open("rb") as f_existing:
                shutil.copyfileobj(f_existing, out)
        if tmp_jsonl.exists():
            with tmp_jsonl.open("rb") as f_new:
                shutil.copyfileobj(f_new, out)

    shutil.copyfile(merged_tmp, volume_json)
    if merged_tmp.exists():
        merged_tmp.unlink()


def _aemet_get_json(
    url: str,
    api_key: str,
    timeout: int = 30,
    max_retries: int = 5,
    base_backoff: float = 2.0,
) -> Optional[Any]:
    # Flujo AEMET OpenData: meta (api_key) -> payload en `datos`. 404 -> None.
    headers = {"api_key": api_key, "cache-control": "no-cache"}

    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, timeout=timeout)

        if resp.status_code == 404:
            return None

        if resp.status_code == 429:
            wait = base_backoff * (2 ** attempt)
            print(f"[429 RATE LIMIT] Esperando {wait:.1f}s antes de reintentar...")
            time.sleep(wait)
            continue

        if 500 <= resp.status_code < 600:
            wait = base_backoff * (2 ** attempt)
            print(f"[{resp.status_code} SERVER ERROR] Esperando {wait:.1f}s...")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        meta = resp.json()
        estado = meta.get("estado")
        datos_url = meta.get("datos")

        if estado == 404 or not datos_url:
            return None

        if estado != 200:
            raise RuntimeError(f"Respuesta AEMET inesperada ({estado}): {meta.get('descripcion')}")

        for data_attempt in range(max_retries):
            data_resp = requests.get(datos_url, timeout=timeout)
            if data_resp.status_code == 429:
                wait = base_backoff * (2 ** data_attempt)
                print(f"[429 EN DATOS] Esperando {wait:.1f}s...")
                time.sleep(wait)
                continue
            data_resp.raise_for_status()
            return data_resp.json()

        raise RuntimeError(f"Agotados reintentos al descargar datos desde {datos_url}")

    raise RuntimeError(f"Agotados reintentos para {url}")


def get_extremadura_stations(
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    provinces: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    # Inventario AEMET filtrado a provincias de Extremadura.
    effective_key = _get_api_key(api_key)
    allowed_provinces = {p.strip().upper() for p in (provinces or DEFAULT_PROVINCES)}
    url = f"{base_url.rstrip('/')}/valores/climatologicos/inventarioestaciones/todasestaciones"

    stations = _aemet_get_json(url=url, api_key=effective_key) or []
    filtered = [
        st for st in stations
        if str(st.get("provincia", "")).strip().upper() in allowed_provinces
    ]
    return filtered


def _iter_date_windows(date_from: str, date_to: str, window_days: int = 180):
    current = _parse_date(date_from)
    end = _parse_date(date_to)
    delta = timedelta(days=max(1, window_days) - 1)

    while current <= end:
        win_end = min(current + delta, end)
        yield current, win_end
        current = win_end + timedelta(days=1)


def fetch_station_daily_block(
    station_id: str,
    date_from: str,
    date_to: str,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    station_meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    # Climatología diaria de una estación (idema) entre YYYY-MM-DD.
    effective_key = _get_api_key(api_key)
    d_ini = _format_aemet_datetime(_parse_date(date_from), end_of_day=False)
    d_fin = _format_aemet_datetime(_parse_date(date_to), end_of_day=True)

    url = (
        f"{base_url.rstrip('/')}/valores/climatologicos/diarios/datos/"
        f"fechaini/{d_ini}/fechafin/{d_fin}/estacion/{station_id}"
    )
    records = _aemet_get_json(url=url, api_key=effective_key) or []
    ingested_at = datetime.utcnow().isoformat()

    enriched: List[Dict[str, Any]] = []
    for row in records:
        item = dict(row)
        item["query_date_from"] = date_from
        item["query_date_to"] = date_to
        item["ingested_at_utc"] = ingested_at
        if station_meta:
            item.setdefault("provincia_estacion", station_meta.get("provincia"))
            item.setdefault("nombre_estacion", station_meta.get("nombre"))
            item.setdefault("altitud_estacion", station_meta.get("altitud"))
        enriched.append(item)

    return enriched


def _ingest_stations_range_to_handle(
    fh,
    stations: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    window_days: int,
    api_key: str,
    base_url: str,
    sleep_seconds: float,
    mode_label: str,
) -> int:
    rows_added = 0
    windows = list(_iter_date_windows(date_from, date_to, window_days=window_days))

    for st in stations:
        idema = str(st.get("indicativo", "")).strip()
        nombre = str(st.get("nombre", "")).strip()
        if not idema:
            continue

        for win_start, win_end in windows:
            s_from = win_start.isoformat()
            s_to = win_end.isoformat()
            try:
                rows = fetch_station_daily_block(
                    station_id=idema,
                    date_from=s_from,
                    date_to=s_to,
                    api_key=api_key,
                    base_url=base_url,
                    station_meta=st,
                )
                if not rows:
                    print(f"[SIN DATOS][{mode_label}] {idema} ({nombre}) {s_from} -> {s_to}")
                else:
                    for r in rows:
                        r["ingest_mode"] = mode_label
                        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                    fh.flush()
                    rows_added += len(rows)
                    print(
                        f"[OK][{mode_label}] {idema} ({nombre}) +{len(rows)} filas "
                        f"(acumulado: {rows_added}) {s_from} -> {s_to}"
                    )
            except Exception as exc:
                print(f"[ERROR][{mode_label}] {idema} ({nombre}) {s_from} -> {s_to}: {exc}")

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    return rows_added


def ingest_aemet_historical_2015_today(
    output_file: str = DEFAULT_OUTPUT_FILE,
    date_from: str = "2015-01-01",
    date_to: Optional[str] = None,
    window_days: int = 180,
    sleep_seconds: float = 1.0,
    reset_file: bool = True,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    provinces: Optional[Iterable[str]] = None,
) -> Dict[str, int]:
    # Histórico diario Extremadura: ventanas de `window_days`, /tmp y volcado al Volume.
    effective_key = _get_api_key(api_key)
    end_str = date_to or date.today().isoformat()
    stations = get_extremadura_stations(
        api_key=effective_key,
        base_url=base_url,
        provinces=provinces,
    )
    print(f"Estaciones encontradas en Extremadura: {len(stations)}")

    tmp_path = Path(f"/tmp/aemet_hist_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl")
    target_path = Path(output_file)

    with tmp_path.open("w", encoding="utf-8") as fh:
        total_rows = _ingest_stations_range_to_handle(
            fh=fh,
            stations=stations,
            date_from=date_from,
            date_to=end_str,
            window_days=window_days,
            api_key=effective_key,
            base_url=base_url,
            sleep_seconds=sleep_seconds,
            mode_label="historical_extremadura",
        )

    _merge_tmp_into_volume(tmp_jsonl=tmp_path, volume_json=target_path, overwrite_volume=reset_file)
    if tmp_path.exists():
        tmp_path.unlink()

    print(f"Bronze AEMET actualizado en {target_path}. Filas descargadas: {total_rows}")
    return {"stations_count": len(stations), "total_rows": total_rows}


def ingest_aemet_incremental(
    date_from: str,
    date_to: Optional[str] = None,
    output_file: str = DEFAULT_OUTPUT_FILE,
    window_days: int = 15,
    sleep_seconds: float = 0.8,
    api_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    provinces: Optional[Iterable[str]] = None,
) -> int:
    # Añade un rango reciente de diarios AEMET al Volume.
    effective_key = _get_api_key(api_key)
    end_str = date_to or date.today().isoformat()
    stations = get_extremadura_stations(
        api_key=effective_key,
        base_url=base_url,
        provinces=provinces,
    )

    tmp_path = Path(f"/tmp/aemet_inc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl")
    target_path = Path(output_file)

    with tmp_path.open("w", encoding="utf-8") as fh:
        added = _ingest_stations_range_to_handle(
            fh=fh,
            stations=stations,
            date_from=date_from,
            date_to=end_str,
            window_days=window_days,
            api_key=effective_key,
            base_url=base_url,
            sleep_seconds=sleep_seconds,
            mode_label="incremental_extremadura",
        )

    if added > 0:
        _merge_tmp_into_volume(tmp_jsonl=tmp_path, volume_json=target_path, overwrite_volume=False)
    if tmp_path.exists():
        tmp_path.unlink()

    print(f"Incremental AEMET ({date_from} -> {end_str}): +{added} filas en {target_path}")
    return added