# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Build
# MAGIC
# MAGIC Production notebook for validating, conforming, and gap-checking turbine measurements.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from colibri_wind.transforms import build_silver_outputs

# COMMAND ----------

config = pipeline_config_from_widgets()
ensure_schemas(config)

# COMMAND ----------

with task_event("build_silver"):
    raw_df = spark.table(config.table_name("bronze", "wind_turbine_raw"))
    outputs = build_silver_outputs(
        raw_df,
        pipeline_run_id=config.run_id,
        total_turbines=config.total_turbines,
        turbines_per_file=config.turbines_per_file,
    )

    overwrite_table(outputs.measurements, config.table_name("silver", "wind_turbine_measurements"))
    overwrite_table(
        outputs.rejects, config.table_name("silver", "wind_turbine_measurement_rejects")
    )
    overwrite_table(outputs.gaps, config.table_name("silver", "wind_turbine_measurement_gaps"))
