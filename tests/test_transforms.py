from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F

from colibri_wind.transforms import (
    build_power_anomalies,
    build_power_summary,
    build_silver_outputs,
)


def test_silver_outputs_clean_reject_impute_dedupe_and_detect_gaps(spark) -> None:
    raw = spark.createDataFrame(
        [
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "1",
                "wind_speed": "10.0",
                "wind_direction": "100",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 1, 0),
            },
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "1",
                "wind_speed": "11.0",
                "wind_direction": "110",
                "power_output": "2.2",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 2, 0),
            },
            {
                "timestamp": "2022-03-01 01:00:00",
                "turbine_id": "1",
                "wind_speed": "",
                "wind_direction": "",
                "power_output": "",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 2, 0),
            },
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "2",
                "wind_speed": "12.0",
                "wind_direction": "120",
                "power_output": "3.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 2, 0),
            },
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "6",
                "wind_speed": "12.0",
                "wind_direction": "120",
                "power_output": "3.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 2, 0),
            },
            {
                "timestamp": "2022-03-01 02:00:00",
                "turbine_id": "2",
                "wind_speed": "12.0",
                "wind_direction": "120",
                "power_output": "99.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
                "ingestion_timestamp": datetime(2022, 3, 1, 2, 0),
            },
        ]
    )

    outputs = build_silver_outputs(raw, pipeline_run_id="run-1", total_turbines=2)

    measurements = {
        (row["turbine_id"], row["timestamp"].hour): row for row in outputs.measurements.collect()
    }
    rejects = outputs.rejects.select(F.explode("validation_errors").alias("error")).collect()
    gap_keys = {
        (row["turbine_id"], row["timestamp"].hour)
        for row in outputs.gaps.select("turbine_id", "timestamp").collect()
    }

    assert measurements[(1, 0)]["power_output"] == 2.2
    assert measurements[(1, 0)]["duplicate_count"] == 2
    assert measurements[(1, 1)]["power_output_was_imputed"] is True
    assert measurements[(1, 1)]["power_output"] == 2.2
    assert "duplicate_turbine_hour" in {row["error"] for row in rejects}
    assert "turbine_file_mismatch" in {row["error"] for row in rejects}
    assert "power_output_out_of_range" in {row["error"] for row in rejects}
    assert (2, 1) in gap_keys


def test_gold_summary(spark) -> None:
    measurements = spark.createDataFrame(
        [
            {
                "timestamp": datetime(2022, 3, 1, 0, 0),
                "measurement_date": datetime(2022, 3, 1).date(),
                "turbine_id": 1,
                "power_output": 2.0,
            },
            {
                "timestamp": datetime(2022, 3, 1, 1, 0),
                "measurement_date": datetime(2022, 3, 1).date(),
                "turbine_id": 1,
                "power_output": 4.0,
            },
            {
                "timestamp": datetime(2022, 3, 1, 0, 0),
                "measurement_date": datetime(2022, 3, 1).date(),
                "turbine_id": 2,
                "power_output": 3.0,
            },
        ]
    )
    rejects = spark.createDataFrame(
        [
            {
                "timestamp": datetime(2022, 3, 1, 0, 0),
                "turbine_id": 1,
            }
        ]
    )
    gaps = spark.createDataFrame(
        [
            {
                "timestamp": datetime(2022, 3, 1, 1, 0),
                "turbine_id": 2,
            }
        ]
    )

    summary = build_power_summary(measurements, rejects, gaps)

    row = summary.where("turbine_id = 1").first()

    assert row["observed_count"] == 2
    assert row["rejected_count"] == 1
    assert row["expected_count"] == 24
    assert len(summary.columns) == len(set(summary.columns))


def test_anomaly_threshold_flags_known_outlier(spark) -> None:
    rows = []
    for turbine_id in range(1, 16):
        rows.append(
            {
                "window_start": datetime(2022, 3, 1, 0, 0),
                "window_end": datetime(2022, 3, 2, 0, 0),
                "turbine_id": turbine_id,
                "avg_power_output_mw": 3.0 if turbine_id < 15 else 10.0,
                "expected_count": 24,
                "observed_count": 24,
                "rejected_count": 0,
                "gap_count": 0,
                "completeness_pct": 100.0,
                "min_power_output_mw": 3.0,
                "max_power_output_mw": 3.0 if turbine_id < 15 else 10.0,
                "stddev_power_output_mw": 0.0,
            }
        )
    summary = spark.createDataFrame(rows)

    anomalies = build_power_anomalies(summary)
    outlier = anomalies.where("turbine_id = 15").first()
    normal = anomalies.where("turbine_id = 1").first()

    assert outlier["is_anomaly"] is True
    assert normal["is_anomaly"] is False
    assert outlier["z_score"] > 2


def test_configured_turbine_range_is_enforced(spark) -> None:
    raw = spark.createDataFrame(
        [
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "3",
                "wind_speed": "10.0",
                "wind_direction": "90",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
            }
        ]
    )

    outputs = build_silver_outputs(raw, pipeline_run_id="run-1", total_turbines=2)
    errors = outputs.rejects.select(F.explode("validation_errors").alias("error")).collect()

    assert {row["error"] for row in errors} == {"invalid_turbine_id"}


def test_non_hourly_timestamp_is_rejected(spark) -> None:
    raw = spark.createDataFrame(
        [
            {
                "timestamp": "2022-03-01 00:15:00",
                "turbine_id": "1",
                "wind_speed": "10.0",
                "wind_direction": "90",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
            }
        ]
    )

    outputs = build_silver_outputs(raw, pipeline_run_id="run-1", total_turbines=1)
    errors = outputs.rejects.select(F.explode("validation_errors").alias("error")).collect()

    assert {row["error"] for row in errors} == {"timestamp_not_on_hour"}


def test_missing_wind_direction_uses_circular_mean(spark) -> None:
    raw = spark.createDataFrame(
        [
            {
                "timestamp": "2022-03-01 00:00:00",
                "turbine_id": "1",
                "wind_speed": "10.0",
                "wind_direction": "359",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
            },
            {
                "timestamp": "2022-03-01 01:00:00",
                "turbine_id": "1",
                "wind_speed": "10.0",
                "wind_direction": "1",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
            },
            {
                "timestamp": "2022-03-01 02:00:00",
                "turbine_id": "1",
                "wind_speed": "10.0",
                "wind_direction": "",
                "power_output": "2.0",
                "source_file": "/Volumes/main/bronze/raw/data_group_1.csv",
            },
        ]
    )

    outputs = build_silver_outputs(raw, pipeline_run_id="run-1", total_turbines=1)
    imputed = outputs.measurements.where("wind_direction_was_imputed").first()

    distance_from_north = min(imputed["wind_direction"], 360 - imputed["wind_direction"])
    assert distance_from_north < 0.01


def test_summary_keeps_turbine_window_with_no_valid_measurements(spark) -> None:
    window_start = datetime(2022, 3, 1)
    measurements = spark.createDataFrame(
        [{"timestamp": window_start, "turbine_id": 1, "power_output": 2.0}]
    )
    rejects = spark.createDataFrame(
        [], "timestamp timestamp, turbine_id int, validation_errors array<string>"
    )
    gaps = spark.createDataFrame(
        [
            {"timestamp": window_start + timedelta(hours=hour), "turbine_id": turbine_id}
            for turbine_id, first_hour in ((1, 1), (2, 0))
            for hour in range(first_hour, 24)
        ]
    )

    summary = build_power_summary(measurements, rejects, gaps)
    anomalies = build_power_anomalies(summary)
    missing_turbine = summary.where("turbine_id = 2").first()
    missing_turbine_anomaly = anomalies.where("turbine_id = 2").first()

    assert missing_turbine["observed_count"] == 0
    assert missing_turbine["gap_count"] == 24
    assert missing_turbine["completeness_pct"] == 0
    assert missing_turbine["avg_power_output_mw"] is None
    assert missing_turbine_anomaly["is_anomaly"] is False
    assert missing_turbine_anomaly["evaluation_status"] == "missing_measurements"
