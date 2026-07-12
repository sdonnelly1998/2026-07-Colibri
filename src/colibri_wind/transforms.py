from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

TOTAL_TURBINES = 15
TURBINES_PER_SOURCE_FILE = 5

# Deliberately broad physical limits. The supplied data sits well inside these,
# but the reject table should catch obvious sensor or parsing failures.
MAX_WIND_SPEED = 60.0
MAX_WIND_DIRECTION = 359
MAX_POWER_OUTPUT = 10.0


@dataclass(frozen=True)
class SilverOutputs:
    measurements: DataFrame
    rejects: DataFrame
    gaps: DataFrame


@dataclass(frozen=True)
class GoldOutputs:
    summary: DataFrame
    anomalies: DataFrame


def build_silver_outputs(
    raw_df: DataFrame,
    *,
    pipeline_run_id: str,
    total_turbines: int = TOTAL_TURBINES,
    turbines_per_file: int = TURBINES_PER_SOURCE_FILE,
) -> SilverOutputs:
    parsed = _parse_raw_measurements(
        raw_df,
        pipeline_run_id,
        total_turbines=total_turbines,
        turbines_per_file=turbines_per_file,
    )
    base_rejects = parsed.filter(F.size("validation_errors") > 0)
    candidate_rows = parsed.filter(F.size("validation_errors") == 0)

    # If the source sends the same turbine-hour twice, keep the latest arrival
    # before calculating imputation values. Older copies remain auditable.
    order_window = Window.partitionBy("timestamp", "turbine_id").orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("source_file_modified_at").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
    )
    ranked_candidates = candidate_rows.withColumn(
        "duplicate_rank", F.row_number().over(order_window)
    ).withColumn(
        "duplicate_count", F.count("*").over(Window.partitionBy("timestamp", "turbine_id"))
    )

    current_rows = ranked_candidates.filter(F.col("duplicate_rank") == 1)
    checked = _finalize_validation_errors(_apply_turbine_day_medians(current_rows))
    measurements = checked.filter(F.size("validation_errors") == 0).select(*_measurement_columns())
    imputation_rejects = checked.filter(F.size("validation_errors") > 0)
    duplicate_rejects = _with_original_values(
        ranked_candidates.filter(F.col("duplicate_rank") > 1).withColumn(
            "validation_errors", F.array(F.lit("duplicate_turbine_hour"))
        )
    )
    rejects = (
        _with_original_values(base_rejects)
        .select(*_reject_columns())
        .unionByName(duplicate_rejects.select(*_reject_columns()))
        .unionByName(imputation_rejects.select(*_reject_columns()))
    )
    gaps = detect_measurement_gaps(measurements, total_turbines=total_turbines)

    return SilverOutputs(measurements=measurements, rejects=rejects, gaps=gaps)


def build_gold_outputs(
    measurements: DataFrame,
    rejects: DataFrame,
    gaps: DataFrame,
    *,
    window_hours: int = 24,
    anomaly_sigma: float = 2.0,
) -> GoldOutputs:
    summary = build_power_summary(measurements, rejects, gaps, window_hours=window_hours)
    anomalies = build_power_anomalies(summary, sigma=anomaly_sigma)
    return GoldOutputs(summary=summary, anomalies=anomalies)


def detect_measurement_gaps(
    measurements: DataFrame, *, total_turbines: int = TOTAL_TURBINES
) -> DataFrame:
    spark = measurements.sparkSession
    bounds = measurements.agg(
        F.min("timestamp").alias("min_ts"), F.max("timestamp").alias("max_ts")
    )
    hours = bounds.where(F.col("min_ts").isNotNull()).select(
        F.explode(F.sequence("min_ts", "max_ts", F.expr("interval 1 hour"))).alias("timestamp")
    )
    turbines = spark.range(1, total_turbines + 1).select(
        F.col("id").cast("int").alias("turbine_id")
    )
    expected = hours.crossJoin(turbines)
    actual = measurements.select("timestamp", "turbine_id").distinct()

    return (
        expected.join(actual, ["timestamp", "turbine_id"], "left_anti")
        .withColumn("measurement_date", F.to_date("timestamp"))
        .withColumn("gap_detected_at", F.current_timestamp())
        .withColumn("missing_reason", F.lit("no_measurement_for_expected_turbine_hour"))
        .select(
            "timestamp",
            "measurement_date",
            "turbine_id",
            "missing_reason",
            "gap_detected_at",
        )
    )


def build_power_summary(
    measurements: DataFrame,
    rejects: DataFrame,
    gaps: DataFrame,
    *,
    window_hours: int = 24,
) -> DataFrame:
    window_expr = F.window("timestamp", f"{window_hours} hours")
    measurement_summary = (
        measurements.groupBy("turbine_id", window_expr.alias("power_window"))
        .agg(
            F.count("*").cast("int").alias("observed_count"),
            F.min("power_output").alias("min_power_output_mw"),
            F.max("power_output").alias("max_power_output_mw"),
            F.avg("power_output").alias("avg_power_output_mw"),
            F.stddev_samp("power_output").alias("stddev_power_output_mw"),
        )
        .select(
            "turbine_id",
            F.col("power_window.start").alias("window_start"),
            F.col("power_window.end").alias("window_end"),
            "observed_count",
            "min_power_output_mw",
            "max_power_output_mw",
            "avg_power_output_mw",
            "stddev_power_output_mw",
        )
    )

    is_superseded = (
        F.array_contains("validation_errors", "duplicate_turbine_hour")
        if "validation_errors" in rejects.columns
        else F.lit(False)
    )
    reject_counts = (
        rejects.where(F.col("timestamp").isNotNull() & F.col("turbine_id").isNotNull())
        .groupBy("turbine_id", window_expr.alias("reject_window"))
        .agg(
            F.sum(F.when(~is_superseded, 1).otherwise(0)).cast("int").alias("rejected_count"),
            F.sum(F.when(is_superseded, 1).otherwise(0)).cast("int").alias("superseded_count"),
        )
        .select(
            "turbine_id",
            F.col("reject_window.start").alias("window_start"),
            F.col("reject_window.end").alias("window_end"),
            "rejected_count",
            "superseded_count",
        )
    )
    gap_counts = (
        gaps.groupBy("turbine_id", window_expr.alias("gap_window"))
        .agg(F.count("*").cast("int").alias("gap_count"))
        .select(
            "turbine_id",
            F.col("gap_window.start").alias("window_start"),
            F.col("gap_window.end").alias("window_end"),
            "gap_count",
        )
    )

    key_columns = ["turbine_id", "window_start", "window_end"]
    window_keys = (
        measurement_summary.select(*key_columns)
        .unionByName(reject_counts.select(*key_columns))
        .unionByName(gap_counts.select(*key_columns))
        .distinct()
    )
    expected_count = F.lit(window_hours).cast("int")
    return (
        window_keys.join(measurement_summary, key_columns, "left")
        .join(reject_counts, key_columns, "left")
        .join(gap_counts, key_columns, "left")
        .fillna(
            {
                "observed_count": 0,
                "rejected_count": 0,
                "superseded_count": 0,
                "gap_count": 0,
            }
        )
        .withColumn("expected_count", expected_count)
        .withColumn("completeness_pct", F.round(F.col("observed_count") / expected_count * 100, 2))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "window_start",
            "window_end",
            "turbine_id",
            "expected_count",
            "observed_count",
            "rejected_count",
            "superseded_count",
            "gap_count",
            "completeness_pct",
            "min_power_output_mw",
            "max_power_output_mw",
            "avg_power_output_mw",
            "stddev_power_output_mw",
            "created_at",
        )
    )


def build_power_anomalies(summary: DataFrame, *, sigma: float = 2.0) -> DataFrame:
    farm_stats = summary.groupBy("window_start", "window_end").agg(
        F.avg("avg_power_output_mw").alias("farm_mean_power_output_mw"),
        F.stddev_samp("avg_power_output_mw").alias("farm_stddev_power_output_mw"),
    )
    joined = summary.join(farm_stats, ["window_start", "window_end"], "inner")
    lower = F.col("farm_mean_power_output_mw") - F.lit(sigma) * F.col("farm_stddev_power_output_mw")
    upper = F.col("farm_mean_power_output_mw") + F.lit(sigma) * F.col("farm_stddev_power_output_mw")
    has_measurement = F.col("avg_power_output_mw").isNotNull()
    can_evaluate = (
        has_measurement
        & F.col("farm_stddev_power_output_mw").isNotNull()
        & (F.col("farm_stddev_power_output_mw") > 0)
    )

    return (
        joined.withColumn("lower_threshold_mw", lower)
        .withColumn("upper_threshold_mw", upper)
        .withColumn(
            "z_score",
            F.when(
                can_evaluate,
                (F.col("avg_power_output_mw") - F.col("farm_mean_power_output_mw"))
                / F.col("farm_stddev_power_output_mw"),
            ),
        )
        .withColumn(
            "is_anomaly",
            F.when(
                can_evaluate,
                (F.col("avg_power_output_mw") < F.col("lower_threshold_mw"))
                | (F.col("avg_power_output_mw") > F.col("upper_threshold_mw")),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "evaluation_status",
            F.when(~has_measurement, F.lit("missing_measurements"))
            .when(can_evaluate, F.lit("evaluated"))
            .otherwise(F.lit("insufficient_variance")),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "window_start",
            "window_end",
            "turbine_id",
            "avg_power_output_mw",
            "farm_mean_power_output_mw",
            "farm_stddev_power_output_mw",
            "lower_threshold_mw",
            "upper_threshold_mw",
            "z_score",
            "is_anomaly",
            "evaluation_status",
            "created_at",
        )
    )


def _parse_raw_measurements(
    raw_df: DataFrame,
    pipeline_run_id: str,
    *,
    total_turbines: int,
    turbines_per_file: int,
) -> DataFrame:
    df = raw_df.select(
        _col_or_null(raw_df, "timestamp").alias("raw_timestamp"),
        _col_or_null(raw_df, "turbine_id").alias("raw_turbine_id"),
        _col_or_null(raw_df, "wind_speed").alias("raw_wind_speed"),
        _col_or_null(raw_df, "wind_direction").alias("raw_wind_direction"),
        _col_or_null(raw_df, "power_output").alias("raw_power_output"),
        _col_or_null(raw_df, "source_file").alias("source_file"),
        _col_or_null(raw_df, "_rescued_data").alias("_rescued_data"),
        F.coalesce(
            F.col("ingestion_timestamp")
            if "ingestion_timestamp" in raw_df.columns
            else F.lit(None),
            F.current_timestamp(),
        ).alias("ingestion_timestamp"),
        (
            F.col("source_file_modified_at").cast("timestamp")
            if "source_file_modified_at" in raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("source_file_modified_at"),
    )

    parsed = (
        df.withColumn("timestamp", F.to_timestamp("raw_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("measurement_date", F.to_date("timestamp"))
        .withColumn("turbine_id", F.col("raw_turbine_id").cast("int"))
        .withColumn("wind_speed_original", F.col("raw_wind_speed").cast("double"))
        .withColumn("wind_direction_original", F.col("raw_wind_direction").cast("double"))
        .withColumn("power_output_original", F.col("raw_power_output").cast("double"))
        .withColumn("source_file_name", F.regexp_extract("source_file", r"([^/]+)$", 1))
        .withColumn("source_group_id", _source_group_from_file())
        .withColumn(
            "expected_source_group_id",
            _expected_group_for_turbine(turbines_per_file),
        )
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    )

    return _with_errors(parsed, _base_validation_errors(total_turbines))


def _apply_turbine_day_medians(df: DataFrame) -> DataFrame:
    clean_candidates = df.filter(F.size("validation_errors") == 0)
    medians = clean_candidates.groupBy("turbine_id", "measurement_date").agg(
        F.expr("percentile_approx(wind_speed_original, 0.5, 100)").alias("median_wind_speed"),
        # Direction wraps at 360 degrees, so a circular mean is safer than a numeric median.
        F.pmod(
            F.degrees(
                F.atan2(
                    F.avg(F.sin(F.radians("wind_direction_original"))),
                    F.avg(F.cos(F.radians("wind_direction_original"))),
                )
            )
            + F.lit(360.0),
            F.lit(360.0),
        ).alias("mean_wind_direction"),
        F.expr("percentile_approx(power_output_original, 0.5, 100)").alias("median_power_output"),
    )
    return (
        df.join(medians, ["turbine_id", "measurement_date"], "left")
        .withColumn("wind_speed", F.coalesce("wind_speed_original", "median_wind_speed"))
        .withColumn("wind_direction", F.coalesce("wind_direction_original", "mean_wind_direction"))
        .withColumn("power_output", F.coalesce("power_output_original", "median_power_output"))
        .withColumn(
            "wind_speed_was_imputed",
            F.col("wind_speed_original").isNull() & F.col("median_wind_speed").isNotNull(),
        )
        .withColumn(
            "wind_direction_was_imputed",
            F.col("wind_direction_original").isNull() & F.col("mean_wind_direction").isNotNull(),
        )
        .withColumn(
            "power_output_was_imputed",
            F.col("power_output_original").isNull() & F.col("median_power_output").isNotNull(),
        )
    )


def _finalize_validation_errors(df: DataFrame) -> DataFrame:
    no_existing_errors = F.size("validation_errors") == 0
    additional_errors = F.array(
        F.when(no_existing_errors & F.col("wind_speed").isNull(), F.lit("missing_wind_speed")),
        F.when(
            no_existing_errors & F.col("wind_direction").isNull(),
            F.lit("missing_wind_direction"),
        ),
        F.when(no_existing_errors & F.col("power_output").isNull(), F.lit("missing_power_output")),
    )
    return (
        df.withColumn("validation_errors", F.concat("validation_errors", additional_errors))
        .withColumn("validation_errors", F.expr("filter(validation_errors, x -> x is not null)"))
        .withColumn("rejected_at", F.current_timestamp())
    )


def _with_original_values(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("wind_speed", F.col("wind_speed_original"))
        .withColumn("wind_direction", F.col("wind_direction_original"))
        .withColumn("power_output", F.col("power_output_original"))
        .withColumn("rejected_at", F.current_timestamp())
    )


def _col_or_null(df: DataFrame, name: str):
    if name in df.columns:
        return F.col(name).cast("string")
    return F.lit(None).cast("string")


def _cast_failed(raw_column: str, cast_column: str):
    return (
        F.col(raw_column).isNotNull()
        & (F.length(F.trim(F.col(raw_column))) > 0)
        & F.col(cast_column).isNull()
    )


def _source_group_from_file() -> Column:
    return F.regexp_extract("source_file_name", r"data_group_(\d+)\.csv$", 1).cast("int")


def _expected_group_for_turbine(turbines_per_file: int) -> Column:
    return F.floor((F.col("turbine_id") - F.lit(1)) / F.lit(turbines_per_file)) + F.lit(1)


def _base_validation_errors(total_turbines: int) -> list[Column]:
    return [
        _when(F.col("timestamp").isNull(), "invalid_timestamp"),
        _when(
            F.col("timestamp").isNotNull()
            & (F.col("timestamp") != F.date_trunc("hour", F.col("timestamp"))),
            "timestamp_not_on_hour",
        ),
        _when(
            F.col("turbine_id").isNull()
            | (F.col("turbine_id") < 1)
            | (F.col("turbine_id") > total_turbines),
            "invalid_turbine_id",
        ),
        _when(F.col("source_group_id").isNull(), "invalid_source_file"),
        _when(
            F.col("source_group_id").isNotNull()
            & F.col("expected_source_group_id").isNotNull()
            & (F.col("source_group_id") != F.col("expected_source_group_id")),
            "turbine_file_mismatch",
        ),
        _when(_cast_failed("raw_wind_speed", "wind_speed_original"), "invalid_wind_speed"),
        _when(
            F.col("wind_speed_original").isNotNull()
            & (
                (F.col("wind_speed_original") < 0) | (F.col("wind_speed_original") > MAX_WIND_SPEED)
            ),
            "wind_speed_out_of_range",
        ),
        _when(
            _cast_failed("raw_wind_direction", "wind_direction_original"),
            "invalid_wind_direction",
        ),
        _when(
            F.col("wind_direction_original").isNotNull()
            & (
                (F.col("wind_direction_original") < 0)
                | (F.col("wind_direction_original") > MAX_WIND_DIRECTION)
            ),
            "wind_direction_out_of_range",
        ),
        _when(_cast_failed("raw_power_output", "power_output_original"), "invalid_power_output"),
        _when(
            F.col("power_output_original").isNotNull()
            & (
                (F.col("power_output_original") < 0)
                | (F.col("power_output_original") > MAX_POWER_OUTPUT)
            ),
            "power_output_out_of_range",
        ),
        _when(
            F.col("_rescued_data").isNotNull() & (F.length(F.trim("_rescued_data")) > 0),
            "rescued_data_present",
        ),
    ]


def _when(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))


def _with_errors(df: DataFrame, error_exprs: list[Column]) -> DataFrame:
    return (
        df.withColumn("_errors", F.array(*error_exprs))
        .withColumn("validation_errors", F.expr("filter(_errors, x -> x is not null)"))
        .drop("_errors")
    )


def _measurement_columns() -> list[str]:
    return [
        "timestamp",
        "measurement_date",
        "turbine_id",
        "source_file",
        "source_group_id",
        "expected_source_group_id",
        "wind_speed",
        "wind_direction",
        "power_output",
        "wind_speed_was_imputed",
        "wind_direction_was_imputed",
        "power_output_was_imputed",
        "duplicate_count",
        "ingestion_timestamp",
        "source_file_modified_at",
        "pipeline_run_id",
    ]


def _reject_columns() -> list[str]:
    return [
        "raw_timestamp",
        "raw_turbine_id",
        "raw_wind_speed",
        "raw_wind_direction",
        "raw_power_output",
        "timestamp",
        "measurement_date",
        "turbine_id",
        "source_file",
        "source_group_id",
        "expected_source_group_id",
        "wind_speed_original",
        "wind_direction_original",
        "power_output_original",
        "wind_speed",
        "wind_direction",
        "power_output",
        "validation_errors",
        "ingestion_timestamp",
        "source_file_modified_at",
        "rejected_at",
        "pipeline_run_id",
    ]
