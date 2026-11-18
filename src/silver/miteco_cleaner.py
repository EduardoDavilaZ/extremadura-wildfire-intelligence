from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def clean_and_save_silver_miteco(
    spark: SparkSession, 
    df_raw: DataFrame, 
    target_table: str = "workspace.default.silver_miteco_incendios"
) -> DataFrame:
    # Filtra estrictamente Extremadura (Badajoz y Cáceres), normaliza códigos y guarda en Delta
    df_clean = (
        df_raw
        .filter(F.col("provincia").isin("6", "06", "10"))
        .withColumn("provincia", F.lpad(F.col("provincia"), 2, "0"))  # Estandariza '6' -> '06'
        .dropDuplicates(["num_parte"])
    )

    (
        df_clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    return df_clean