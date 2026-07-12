# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Build
# MAGIC
# MAGIC Production notebook for 24-hour turbine summaries and anomaly flags.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from colibri_wind.transforms import build_gold_outputs

# COMMAND ----------

config = pipeline_config_from_widgets()
ensure_schemas(config)

# COMMAND ----------

with task_event("build_gold"):
    measurements = spark.table(config.table_name("silver", "wind_turbine_measurements"))
    rejects = spark.table(config.table_name("silver", "wind_turbine_measurement_rejects"))
    gaps = spark.table(config.table_name("silver", "wind_turbine_measurement_gaps"))

    outputs = build_gold_outputs(
        measurements,
        rejects,
        gaps,
        window_hours=config.window_hours,
        anomaly_sigma=config.anomaly_sigma,
    )

    overwrite_table(outputs.summary, config.table_name("gold", "turbine_power_summary_24h"))
    overwrite_table(outputs.anomalies, config.table_name("gold", "turbine_power_anomalies_24h"))
