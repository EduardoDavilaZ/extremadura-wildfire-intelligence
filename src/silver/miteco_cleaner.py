# silver/miteco_cleaner.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_SILVER_MITECO_TABLE = "workspace.default.silver_miteco_incendios"


def transform_silver_miteco(df_raw: DataFrame) -> DataFrame:
    return (
        df_raw
        .withColumn("provincia_clean", F.trim(F.col("provincia")))
        .filter(F.col("provincia_clean").isin("6", "06", "10"))
        .withColumn("provincia", F.lpad(F.col("provincia_clean"), 2, "0"))
        .drop("provincia_clean")
        .dropDuplicates(["anio", "provincia", "num_parte"])
    )


def save_silver_miteco(
    df_silver: DataFrame,
    target_table: str = DEFAULT_SILVER_MITECO_TABLE,
) -> DataFrame:
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    return df_silver


def clean_and_save_silver_miteco(
    spark: SparkSession,
    df_raw: DataFrame,
    target_table: str = DEFAULT_SILVER_MITECO_TABLE,
) -> DataFrame:
    df_clean = transform_silver_miteco(df_raw)
    return save_silver_miteco(df_clean, target_table=target_table)