from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.catalog import EXTREMADURA_PROVINCE_CODES, SILVER_MITECO_TABLE
from src.delta import save_delta_table

DEFAULT_SILVER_MITECO_TABLE = SILVER_MITECO_TABLE
DEDUP_KEYS = ["anio", "provincia", "num_parte"]
_STRING_COLS = ("municipio", "paraje", "latitud", "longitud")


def _blank_to_null(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def transform_silver_miteco(df_raw: DataFrame) -> DataFrame:
    df = df_raw
    for col_name in _STRING_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, _blank_to_null(col_name))

    provincia_norm = F.lpad(F.trim(F.col("provincia")), 2, "0")
    return (
        df.withColumn("provincia", provincia_norm)
        .filter(F.col("provincia").isin(list(EXTREMADURA_PROVINCE_CODES)))
        .dropDuplicates(DEDUP_KEYS)
    )


def save_silver_miteco(
    df_silver: DataFrame,
    target_table: str = DEFAULT_SILVER_MITECO_TABLE,
) -> DataFrame:
    return save_delta_table(df_silver, target_table)


def build_silver_miteco(
    df_raw: DataFrame,
    target_table: str = DEFAULT_SILVER_MITECO_TABLE,
) -> DataFrame:
    return save_silver_miteco(transform_silver_miteco(df_raw), target_table=target_table)
