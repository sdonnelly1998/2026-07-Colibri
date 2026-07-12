from __future__ import annotations

from pyspark.sql import types as T


def raw_csv_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("turbine_id", T.StringType(), True),
            T.StructField("wind_speed", T.StringType(), True),
            T.StructField("wind_direction", T.StringType(), True),
            T.StructField("power_output", T.StringType(), True),
        ]
    )
