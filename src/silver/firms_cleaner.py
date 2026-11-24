from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def cast_and_parse_timestamps(df: DataFrame) -> DataFrame:
    # acq_time -> char(4) ("HHMM")
    acq_time_4d = F.lpad(
        F.regexp_replace(F.col("acq_time").cast("string"), r"\.0$", ""),
        4,
        "0",
    )

    # acq_date -> date
    acq_date_dt = F.to_date(F.col("acq_date").cast("string"), "yyyy-MM-dd")

    # get hh & mm for clean UTC timestamp
    hh = F.substring(acq_time_4d, 1, 2)
    mm = F.substring(acq_time_4d, 3, 2)
    event_ts_utc = F.to_timestamp(
        F.concat_ws(
            " ",
            F.date_format(acq_date_dt, "yyyy-MM-dd"),
            F.concat_ws(":", hh, mm, F.lit("00")),
        ),
        "yyyy-MM-dd HH:mm:ss",
    )

    return (
        df.withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("bright_ti4", F.col("bright_ti4").cast("double"))
        .withColumn("bright_ti5", F.col("bright_ti5").cast("double"))
        .withColumn("scan", F.col("scan").cast("double"))
        .withColumn("track", F.col("track").cast("double"))
        .withColumn("frp", F.col("frp").cast("double"))
        .withColumn("acq_date", acq_date_dt)
        .withColumn("acq_time_hhmm", acq_time_4d)
        .withColumn("event_timestamp_utc", event_ts_utc)
        .withColumn("ingested_at_utc", F.to_timestamp(F.col("ingested_at_utc")))
    )

def enrich_confidence(df: DataFrame) -> DataFrame:
    conf_raw = F.lower(F.trim(F.col("confidence").cast("string")))

    confidence_label = (F
        .when(conf_raw.isin("l", "low"), F.lit("low"))
        .when(conf_raw.isin("n", "nominal"), F.lit("nominal"))
        .when(conf_raw.isin("h", "high"), F.lit("high"))
        .otherwise(F.lit("unknown"))
    )

    confidence_level = (F
        .when(confidence_label == "low", F.lit(1))
        .when(confidence_label == "nominal", F.lit(2))
        .when(confidence_label == "high", F.lit(3))
        .otherwise(F.lit(None).cast("int"))    
    )

    return (
        df.withColumn("confidence_label", confidence_label)
        .withColumn("confidence_level", confidence_level)
    )

def filter_valid_detections(df: DataFrame) -> DataFrame:
    filtered = df.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("acq_date").isNotNull()
        & F.col("latitude").between(37.94, 40.49)
        & F.col("longitude").between(-7.55, -4.65)
    )

    if "type" in filtered.columns:
        filtered = filtered.filter(F.col("type").cast("int") == 0)

    return filtered

def deduplicate_detections(df: DataFrame) -> DataFrame:
    key_cols = ["event_timestamp_utc", "latitude", "longitude"]
    for optional_col in ["satellite", "instrument", "version"]:
        if optional_col in df.columns:
            key_cols.append(optional_col)

    order_exprs = [
        F.col("confidence_level").desc_nulls_last(),
        F.col("frp").desc_nulls_last(),
    ]
    if "ingested_at_utc" in df.columns:
        order_exprs.append(F.col("ingested_at_utc").desc_nulls_last())

    window_spec = Window.partitionBy(*key_cols).orderBy(*order_exprs)

    return (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

def write_firms_silver_table(
    df: DataFrame,
    table_name: str = "workspace.default.silver_nasa_firms",
) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

def clean_firms_silver(df: DataFrame) -> DataFrame:
    return (
        df.transform(cast_and_parse_timestamps)
        .transform(enrich_confidence)
        .transform(filter_valid_detections)
        .transform(deduplicate_detections)
    )
