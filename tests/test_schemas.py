from __future__ import annotations

from colibri_wind.schemas import raw_csv_schema


def test_raw_csv_schema_keeps_source_values_as_strings() -> None:
    schema = raw_csv_schema()

    assert schema.fieldNames() == [
        "timestamp",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
    ]
    assert {field.dataType.simpleString() for field in schema.fields} == {"string"}
