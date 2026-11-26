from pyspark.sql import DataFrame


def save_delta_table(df: DataFrame, table_name: str, overwrite_schema: bool = True) -> DataFrame:
    writer = df.write.format("delta").mode("overwrite")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_name)
    return df
