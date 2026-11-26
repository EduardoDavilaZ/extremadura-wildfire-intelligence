import xml.etree.ElementTree as ET
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.catalog import BRONZE_MITECO_PATH

DEFAULT_XML_PATH = BRONZE_MITECO_PATH


def _clean_str(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value) -> Optional[int]:
    text = _clean_str(value)
    return int(text) if text and text.isdigit() else None


def parse_miteco_xml_to_df(spark: SparkSession, xml_path: str = DEFAULT_XML_PATH) -> DataFrame:
    # Extrae partes Pif del XML MITECO a un DataFrame tipado.
    root = ET.parse(xml_path).getroot()
    rows = []

    for pif in root.findall(".//{*}Pif"):
        id_pif = pif.findtext("{*}idpif") or pif.findtext("{*}IdPif")
        num_parte = pif.findtext("{*}numeroparte") or pif.findtext("{*}NumeroParte")

        comun = pif.find("{*}pif_comun")
        anio = comun.findtext("{*}anio") if comun is not None else None
        if not _clean_str(num_parte) and comun is not None:
            num_parte = comun.findtext("{*}numeroparte")

        loc = pif.find("{*}pif_localizacion")
        provincia = loc.findtext("{*}idprovincia") if loc is not None else None
        municipio = loc.findtext("{*}idmunicipio") if loc is not None else None
        paraje = loc.findtext("{*}paraje") if loc is not None else None
        lat = loc.findtext("{*}latitud") if loc is not None else None
        lon = loc.findtext("{*}longitud") if loc is not None else None

        rows.append((
            _to_int(id_pif),
            _to_int(num_parte),
            _to_int(anio),
            _clean_str(provincia),
            _clean_str(municipio),
            _clean_str(paraje),
            _clean_str(lat),
            _clean_str(lon),
        ))

    schema = StructType([
        StructField("id_pif", IntegerType(), True),
        StructField("num_parte", IntegerType(), True),
        StructField("anio", IntegerType(), True),
        StructField("provincia", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("paraje", StringType(), True),
        StructField("latitud", StringType(), True),
        StructField("longitud", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)
