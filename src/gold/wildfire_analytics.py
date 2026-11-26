from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.catalog import GOLD_MITECO_ANNUAL_TABLE, SILVER_MITECO_TABLE
from src.delta import save_delta_table

DEFAULT_SILVER_MITECO_TABLE = SILVER_MITECO_TABLE
DEFAULT_GOLD_MITECO_TABLE = GOLD_MITECO_ANNUAL_TABLE


def transform_gold_annual_summary(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver.groupBy("anio", "provincia")
        .agg(F.count("num_parte").alias("total_incendios"))
        .orderBy("anio", "provincia")
    )


def save_gold_annual_summary(
    df_gold: DataFrame,
    target_table: str = DEFAULT_GOLD_MITECO_TABLE,
) -> DataFrame:
    return save_delta_table(df_gold, target_table, overwrite_schema=False)


def build_gold_annual_summary(
    spark: SparkSession,
    silver_miteco_table: str = DEFAULT_SILVER_MITECO_TABLE,
    target_gold_table: str = DEFAULT_GOLD_MITECO_TABLE,
) -> DataFrame:
    df_gold = transform_gold_annual_summary(spark.read.table(silver_miteco_table))
    return save_gold_annual_summary(df_gold, target_table=target_gold_table)
