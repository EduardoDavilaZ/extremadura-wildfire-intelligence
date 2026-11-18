from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def build_gold_annual_summary(
    spark: SparkSession,
    silver_miteco_table: str = "workspace.default.silver_miteco_incendios",
    target_gold_table: str = "workspace.default.gold_miteco_resumen_anual"
) -> DataFrame:
    # Genera el resumen anual de incendios por provincia en Extremadura
    df_silver = spark.read.table(silver_miteco_table)

    df_gold = (
        df_silver
        .groupBy("anio", "provincia")
        .agg(F.count("num_parte").alias("total_incendios"))
        .orderBy("anio", "provincia")
    )

    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_gold_table)
    )

    return df_gold