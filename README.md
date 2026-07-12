# Colibri Wind Turbine Pipeline

This is my solution to the Colibri wind turbine coding exercise. It uses Databricks notebooks and
PySpark to process the supplied turbine CSV files and store the results as Delta tables.

The Databricks bundle deploys one Lakeflow Job with three tasks:

1. **Bronze** incrementally loads CSV files with Auto Loader and retains the source values and file
   details.
2. **Silver** validates and cleans the readings, removes duplicates and records rejected rows and
   missing turbine-hours.
3. **Gold** calculates 24-hour turbine statistics and flags outputs outside the farm mean plus or
   minus two standard deviations.

The notebooks are the production entry points. The transformations are kept in normal Python modules
under `src/` so they can be tested without running the full Databricks job.

## Cleaning Choices

- Source fields are read as strings before casting so malformed values can be rejected explicitly.
- A turbine must appear in its expected group file.
- Invalid timestamps, turbine IDs and physically implausible values are written to the rejects table.
- Isolated numeric nulls use the turbine-day median. Wind direction uses a circular mean.
- For duplicate turbine-hours, the latest ingested row is kept.
- Missing turbine-hours are recorded as gaps rather than replaced with invented measurements.

## Outputs

- `bronze.wind_turbine_raw`
- `silver.wind_turbine_measurements`
- `silver.wind_turbine_measurement_rejects`
- `silver.wind_turbine_measurement_gaps`
- `gold.turbine_power_summary_24h`
- `gold.turbine_power_anomalies_24h`

## Repository

- `notebooks/jobs/` contains the three job notebooks.
- `src/colibri_wind/` contains the schemas and PySpark transformations.
- `resources/` and `config/` contain the bundle resources and dev/prod target settings.
- `tests/` contains focused PySpark tests for parsing, cleaning, gaps, summaries and anomalies.
- `data/raw/` contains the supplied sample files.

The dev target uses `_dev` schemas, while prod uses the stable schema names. The catalog is supplied
when the bundle is deployed, so no workspace address, profile or credential is stored in the repo.

## Assumptions

- Turbines 1-5 belong to group 1, 6-10 to group 2 and 11-15 to group 3.
- Measurements are expected once per hour in UTC.
- Valid ranges are 0-60 m/s for wind speed, 0-359 degrees for direction and 0-10 MW for power.
- The expected date range is taken from the earliest and latest valid farm measurements.
