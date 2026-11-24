import json
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.secrets import get_secret  # noqa: E402

DEFAULT_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
DEFAULT_SOURCE = "VIIRS_SNPP_SP"
DEFAULT_NRT_SOURCE = "VIIRS_NOAA20_NRT"
DEFAULT_BBOX = "-7.55,37.94,-4.65,40.49"
DEFAULT_OUTPUT_FILE = "/Volumes/workspace/default/bronze_nasa_firms/data-2015-2026.json"


def _get_map_key(map_key: Optional[str] = None) -> str:
    key = (map_key or get_secret("FIRMS_MAP_KEY")).strip()
    if not key:
        raise ValueError("No se encontró FIRMS_MAP_KEY en Databricks Secrets.")
    return key


def _parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def _merge_tmp_into_volume(tmp_jsonl: Path, volume_json: Path, overwrite_volume: bool = False) -> None:
    """
    Combina `tmp_jsonl` (en almacenamiento local /tmp) con `volume_json` (en Volume)
    evitando operaciones de `append` (`O_APPEND` / `seek`) directas sobre FUSE.
    """
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


def fetch_firms_block(
    start_date_str: str,
    day_range: int = 5,
    map_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    source: str = DEFAULT_SOURCE,
    bbox: str = DEFAULT_BBOX,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Descarga un bloque de `day_range` días desde `start_date_str` (YYYY-MM-DD)
    y devuelve un DataFrame de pandas con metadatos de ingesta.
    """
    effective_key = _get_map_key(map_key)
    url = f"{base_url.rstrip('/')}/{effective_key}/{source}/{bbox}/{day_range}/{start_date_str}"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    csv_text = response.text.strip()
    if not csv_text or csv_text.lower().startswith(("invalid", "error")):
        raise RuntimeError(f"Respuesta inesperada en {start_date_str} [{source}]: {csv_text[:200]}")

    pdf = pd.read_csv(StringIO(csv_text))
    if not pdf.empty:
        pdf["firms_source"] = source
        pdf["bbox"] = bbox
        pdf["query_start_date"] = start_date_str
        pdf["query_day_range"] = int(day_range)
        pdf["ingested_at_utc"] = datetime.utcnow().isoformat()

    return pdf


def _ingest_range_to_handle(
    fh,
    date_from: str,
    date_to: str,
    step_days: int,
    map_key: Optional[str],
    base_url: str,
    source: str,
    bbox: str,
    sleep_seconds: float,
    splice_label: Optional[str] = None,
) -> int:
    current = _parse_date(date_from)
    end = _parse_date(date_to)
    rows_added = 0
    tag = f" ({splice_label})" if splice_label else ""

    while current <= end:
        start_str = current.isoformat()
        try:
            pdf = fetch_firms_block(
                start_date_str=start_str,
                day_range=step_days,
                map_key=map_key,
                base_url=base_url,
                source=source,
                bbox=bbox,
            )

            if pdf.empty:
                print(f"[SIN DATOS]{tag} {source} desde {start_str} ({step_days}d)")
            else:
                if splice_label:
                    pdf["splice_segment"] = splice_label
                records = pdf.to_dict(orient="records")
                for row in records:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                fh.flush()
                rows_added += len(records)
                print(
                    f"[OK]{tag} {source} +{len(records)} filas "
                    f"(acumulado tramo: {rows_added}) desde {start_str}"
                )

        except Exception as exc:
            print(f"[ERROR]{tag} {source} desde {start_str} ({step_days}d): {exc}")

        current += timedelta(days=step_days)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return rows_added


def ingest_firms_spliced_2015_2026(
    output_file: str = DEFAULT_OUTPUT_FILE,
    step_days: int = 5,
    map_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    bbox: str = DEFAULT_BBOX,
    sleep_seconds: float = 0.5,
    reset_file: bool = True,
    nrt_date_to: Optional[str] = None,
    segments: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """
    Ejecuta el barrido histórico por empalme en `/tmp` y luego vuelca el resultado
    al archivo final del Volume sin usar `append` sobre FUSE.
    """
    if segments is None:
        nrt_from = "2026-08-01"
        nrt_to = nrt_date_to or date.today().isoformat()
        if _parse_date(nrt_to) < _parse_date(nrt_from):
            nrt_to = nrt_from

        segments = [
            {
                "name": "historical_sp",
                "source": "VIIRS_SNPP_SP",
                "date_from": "2015-01-01",
                "date_to": "2026-07-31",
            },
            {
                "name": "recent_nrt",
                "source": DEFAULT_NRT_SOURCE,
                "date_from": nrt_from,
                "date_to": nrt_to,
            },
        ]

    tmp_path = Path(f"/tmp/firms_spliced_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl")
    target_path = Path(output_file)

    summary: Dict[str, int] = {}
    grand_total = 0

    with tmp_path.open("w", encoding="utf-8") as fh:
        for seg in segments:
            seg_name = seg["name"]
            seg_source = seg["source"]
            seg_from = seg["date_from"]
            seg_to = seg["date_to"]

            print(f"=== Iniciando tramo '{seg_name}' [{seg_source}]: {seg_from} -> {seg_to} ===")
            added = _ingest_range_to_handle(
                fh=fh,
                date_from=seg_from,
                date_to=seg_to,
                step_days=step_days,
                map_key=map_key,
                base_url=base_url,
                source=seg_source,
                bbox=bbox,
                sleep_seconds=sleep_seconds,
                splice_label=seg_name,
            )
            summary[seg_name] = added
            grand_total += added
            print(f"=== Fin tramo '{seg_name}': {added} filas ===\n")

    _merge_tmp_into_volume(tmp_jsonl=tmp_path, volume_json=target_path, overwrite_volume=reset_file)
    if tmp_path.exists():
        tmp_path.unlink()

    summary["total_rows"] = grand_total
    print(f"Bronze actualizado en {target_path}. Filas descargadas: {grand_total}")
    return summary


def ingest_firms_incremental(
    date_from: str,
    date_to: Optional[str] = None,
    output_file: str = DEFAULT_OUTPUT_FILE,
    source: str = DEFAULT_NRT_SOURCE,
    splice_label: str = "incremental_nrt",
    step_days: int = 1,
    map_key: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    bbox: str = DEFAULT_BBOX,
    sleep_seconds: float = 0.4,
) -> int:
    """
    Descarga solo un rango reciente (ej. los últimos días tras la última fecha en Bronze)
    en `/tmp` y lo añade al archivo JSON del Volume de forma segura.
    """
    end_str = date_to or date.today().isoformat()
    tmp_path = Path(f"/tmp/firms_inc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl")
    target_path = Path(output_file)

    with tmp_path.open("w", encoding="utf-8") as fh:
        added = _ingest_range_to_handle(
            fh=fh,
            date_from=date_from,
            date_to=end_str,
            step_days=step_days,
            map_key=map_key,
            base_url=base_url,
            source=source,
            bbox=bbox,
            sleep_seconds=sleep_seconds,
            splice_label=splice_label,
        )

    if added > 0:
        _merge_tmp_into_volume(tmp_jsonl=tmp_path, volume_json=target_path, overwrite_volume=False)
    if tmp_path.exists():
        tmp_path.unlink()

    print(f"Incremental completado ({date_from} -> {end_str}): +{added} filas en {target_path}")
    return added