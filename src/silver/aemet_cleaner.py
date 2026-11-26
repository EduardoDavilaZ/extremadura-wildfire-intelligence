from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

from src.catalog import BRONZE_AEMET_PATH, SILVER_AEMET_TABLE
from src.delta import save_delta_table

DEFAULT_BRONZE_PATH = BRONZE_AEMET_PATH
DEFAULT_SILVER_TABLE = SILVER_AEMET_TABLE


def _col_or_null(df: DataFrame, col_name: str, dtype: str = "string"):
    if col_name in df.columns:
        return F.col(col_name).cast(dtype)
    return F.lit(None).cast(dtype)


def _to_double_comma(df: DataFrame, col_name: str):
    raw_str = _col_or_null(df, col_name, "string")
    cleaned = F.trim(F.regexp_replace(raw_str, ",", "."))
    return F.when(cleaned.rlike(r"^-?\d+(\.\d+)?$"), cleaned.cast("double"))


def transform_silver_aemet(df_raw: DataFrame) -> DataFrame:
    prec_str = F.trim(_col_or_null(df_raw, "prec", "string"))
    prec_is_ip = F.upper(prec_str) == "IP"
    prec_num = F.when(prec_is_ip, F.lit(0.0)).otherwise(_to_double_comma(df_raw, "prec"))

    dir_raw_num = _to_double_comma(df_raw, "dir")
    dir_is_variable = dir_raw_num == F.lit(99.0)
    dir_clean_deg = F.when(dir_is_variable, F.lit(None).cast("double")).otherwise(dir_raw_num)

    df_typed = (
        df_raw
        .withColumn("fecha_dt", F.to_date(_col_or_null(df_raw, "fecha", "string")))
        .withColumn("ingested_at_ts", F.to_timestamp(_col_or_null(df_raw, "ingested_at_utc", "string")))
        .withColumn("indicativo", F.trim(_col_or_null(df_raw, "indicativo", "string")))
        .withColumn(
            "provincia",
            F.coalesce(
                F.nullif(F.trim(_col_or_null(df_raw, "provincia_estacion", "string")), F.lit("")),
                F.nullif(F.trim(_col_or_null(df_raw, "provincia", "string")), F.lit("")),
            ),
        )
        .withColumn(
            "nombre_estacion",
            F.coalesce(
                F.nullif(F.trim(_col_or_null(df_raw, "nombre_estacion", "string")), F.lit("")),
                F.nullif(F.trim(_col_or_null(df_raw, "nombre", "string")), F.lit("")),
            ),
        )
        .withColumn(
            "altitud_m",
            F.coalesce(
                _to_double_comma(df_raw, "altitud_estacion"),
                _to_double_comma(df_raw, "altitud"),
            ),
        )
        .withColumn("tmed_c", _to_double_comma(df_raw, "tmed"))
        .withColumn("tmin_c", _to_double_comma(df_raw, "tmin"))
        .withColumn("tmax_c", _to_double_comma(df_raw, "tmax"))
        .withColumn("prec_inapreciable", prec_is_ip)
        .withColumn("prec_mm", prec_num)
        .withColumn("velmedia_ms", _to_double_comma(df_raw, "velmedia"))
        .withColumn("racha_ms", _to_double_comma(df_raw, "racha"))
        .withColumn("dir_deg", dir_clean_deg)
        .withColumn("dir_variable", dir_is_variable)
        .withColumn("hr_media_pct", _to_double_comma(df_raw, "hrMedia"))
        .withColumn("hr_max_pct", _to_double_comma(df_raw, "hrMax"))
        .withColumn("hr_min_pct", _to_double_comma(df_raw, "hrMin"))
        .withColumn("sol_h", _to_double_comma(df_raw, "sol"))
        .withColumn("hora_hr_max", _col_or_null(df_raw, "horaHrMax", "string"))
        .withColumn("hora_hr_min", _col_or_null(df_raw, "horaHrMin", "string"))
        .withColumn("hora_racha", _col_or_null(df_raw, "horaracha", "string"))
        .withColumn("hora_tmax", _col_or_null(df_raw, "horatmax", "string"))
        .withColumn("hora_tmin", _col_or_null(df_raw, "horatmin", "string"))
        .withColumn("ingest_mode", _col_or_null(df_raw, "ingest_mode", "string"))
    )

    w_dedup = Window.partitionBy("indicativo", "fecha_dt").orderBy(F.col("ingested_at_ts").desc_nulls_last())
    df_dedup = (
        df_typed
        .filter(F.col("indicativo").isNotNull() & F.col("fecha_dt").isNotNull())
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_dry_flag = df_dedup.withColumn(
        "es_dia_seco",
        F.when(F.col("prec_mm").isNull(), F.lit(None).cast("int"))
        .when(F.col("prec_mm") < F.lit(0.2), F.lit(1))
        .otherwise(F.lit(0)),
    )

    w_station_time = Window.partitionBy("indicativo").orderBy("fecha_dt")
    # eqNullSafe evita fusionar rachas separadas por días con precipitación NULL.
    df_streaks = (
        df_dry_flag
        .withColumn(
            "cambio_racha",
            F.when(
                ~F.col("es_dia_seco").eqNullSafe(F.lag("es_dia_seco").over(w_station_time)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("grupo_racha", F.sum("cambio_racha").over(w_station_time))
    )

    w_streak_run = Window.partitionBy("indicativo", "grupo_racha").orderBy("fecha_dt")
    w_7d = Window.partitionBy("indicativo").orderBy("fecha_dt").rowsBetween(-6, 0)
    w_14d = Window.partitionBy("indicativo").orderBy("fecha_dt").rowsBetween(-13, 0)
    w_30d = Window.partitionBy("indicativo").orderBy("fecha_dt").rowsBetween(-29, 0)

    return (
        df_streaks
        .withColumn(
            "dias_secos_consecutivos",
            F.when(F.col("es_dia_seco") == 1, F.row_number().over(w_streak_run)).otherwise(F.lit(0)),
        )
        .withColumn("prec_acum_7d_mm", F.round(F.sum("prec_mm").over(w_7d), 2))
        .withColumn("prec_acum_14d_mm", F.round(F.sum("prec_mm").over(w_14d), 2))
        .withColumn("prec_acum_30d_mm", F.round(F.sum("prec_mm").over(w_30d), 2))
        .withColumn("dias_secos_30d", F.sum("es_dia_seco").over(w_30d))
        .withColumn("tmax_media_7d_c", F.round(F.avg("tmax_c").over(w_7d), 2))
        .withColumn("hr_min_media_7d_pct", F.round(F.avg("hr_min_pct").over(w_7d), 2))
        .withColumn(
            "indice_sequedad_termohigro",
            F.when(
                F.col("tmax_c").isNotNull() & F.col("hr_min_pct").isNotNull(),
                F.round(F.col("tmax_c") * (F.lit(1.0) - (F.col("hr_min_pct") / F.lit(100.0))), 2),
            ),
        )
        .select(
            "indicativo",
            "nombre_estacion",
            "provincia",
            "altitud_m",
            "fecha_dt",
            "tmed_c",
            "tmin_c",
            "tmax_c",
            "prec_mm",
            "prec_inapreciable",
            "velmedia_ms",
            "racha_ms",
            "dir_deg",
            "dir_variable",
            "hr_media_pct",
            "hr_max_pct",
            "hr_min_pct",
            "sol_h",
            "es_dia_seco",
            "dias_secos_consecutivos",
            "prec_acum_7d_mm",
            "prec_acum_14d_mm",
            "prec_acum_30d_mm",
            "dias_secos_30d",
            "tmax_media_7d_c",
            "hr_min_media_7d_pct",
            "indice_sequedad_termohigro",
            "hora_hr_max",
            "hora_hr_min",
            "hora_racha",
            "hora_tmax",
            "hora_tmin",
            "ingest_mode",
            "ingested_at_ts",
        )
    )


def save_silver_aemet(df_silver: DataFrame, silver_table: str = DEFAULT_SILVER_TABLE) -> DataFrame:
    return save_delta_table(df_silver, silver_table)


def build_silver_aemet(
    spark: SparkSession,
    bronze_path: str = DEFAULT_BRONZE_PATH,
    silver_table: str = DEFAULT_SILVER_TABLE,
) -> DataFrame:
    df_silver = transform_silver_aemet(spark.read.json(bronze_path))
    return save_silver_aemet(df_silver, silver_table=silver_table)
