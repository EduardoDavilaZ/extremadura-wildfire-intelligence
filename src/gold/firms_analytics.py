from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.catalog import (
    FIRMS_GRID_STEP,
    GOLD_FIRMS_GRID_TABLE,
    GOLD_FIRMS_MONTHLY_TABLE,
    GOLD_FIRMS_PREV_7D_TABLE,
)
from src.delta import save_delta_table

DEFAULT_GRID_STEP = FIRMS_GRID_STEP
SECONDS_PER_DAY = 86400


def add_grid_cells(df: DataFrame, step: float = DEFAULT_GRID_STEP) -> DataFrame:
    return (
        df.withColumn("lat_cell", F.round(F.round(F.col("latitude") / step) * step, 4))
        .withColumn("lon_cell", F.round(F.round(F.col("longitude") / step) * step, 4))
    )


def build_firms_monthly(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver.withColumn("year", F.year("acq_date"))
        .withColumn("month", F.month("acq_date"))
        .groupBy("year", "month")
        .agg(
            F.count("*").alias("detections"),
            F.countDistinct("acq_date").alias("active_days"),
            F.round(F.avg("frp"), 2).alias("frp_avg"),
            F.round(F.max("frp"), 2).alias("frp_max"),
            F.round(F.sum("frp"), 2).alias("frp_sum"),
            F.sum(F.when(F.col("daynight") == "D", 1).otherwise(0)).alias("day_det"),
            F.sum(F.when(F.col("daynight") == "N", 1).otherwise(0)).alias("night_det"),
        )
        .orderBy("year", "month")
    )


def build_firms_grid(df_silver: DataFrame, step: float = DEFAULT_GRID_STEP) -> DataFrame:
    return (
        add_grid_cells(df_silver, step=step)
        .groupBy("lat_cell", "lon_cell")
        .agg(
            F.count("*").alias("detections"),
            F.countDistinct("acq_date").alias("active_days"),
            F.round(F.sum("frp"), 2).alias("frp_sum"),
            F.round(F.max("frp"), 2).alias("frp_max"),
        )
        .orderBy(F.desc("frp_sum"))
    )


def build_firms_prev_7d(df_silver: DataFrame, step: float = DEFAULT_GRID_STEP) -> DataFrame:
    df_cell_daily = (
        add_grid_cells(df_silver, step=step)
        .groupBy("lat_cell", "lon_cell", "acq_date")
        .agg(
            F.count("*").alias("day_detections"),
            F.round(F.sum("frp"), 2).alias("day_frp_sum"),
        )
    )

    # Ventana en segundos Unix: 7 días naturales previos, excluye el día actual.
    w_7d_prev = (
        Window.partitionBy("lat_cell", "lon_cell")
        .orderBy(F.col("acq_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * SECONDS_PER_DAY, -1)
    )

    return (
        df_cell_daily
        .withColumn(
            "prev_7d_frp_sum",
            F.coalesce(F.round(F.sum("day_frp_sum").over(w_7d_prev), 2), F.lit(0.0)),
        )
        .withColumn(
            "prev_7d_detections",
            F.coalesce(F.sum("day_detections").over(w_7d_prev), F.lit(0).cast("bigint")),
        )
    )


def save_gold_firms_monthly(df: DataFrame, table_name: str = GOLD_FIRMS_MONTHLY_TABLE) -> DataFrame:
    return save_delta_table(df, table_name)


def save_gold_firms_grid(df: DataFrame, table_name: str = GOLD_FIRMS_GRID_TABLE) -> DataFrame:
    return save_delta_table(df, table_name)


def save_gold_firms_prev_7d(df: DataFrame, table_name: str = GOLD_FIRMS_PREV_7D_TABLE) -> DataFrame:
    return save_delta_table(df, table_name)
