# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC Production notebook for loading appended CSV files into the bronze Delta
# MAGIC table with Auto Loader.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pyspark.sql import functions as F

from colibri_wind.schemas import raw_csv_schema

# COMMAND ----------

config = pipeline_config_from_widgets()

if not config.raw_path or not config.checkpoint_path:
    raise ValueError("raw_path and checkpoint_path are required for bronze ingestion")

ensure_schemas(config)

# COMMAND ----------

with task_event("ingest_bronze"):
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .schema(raw_csv_schema())
        .load(config.raw_path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(config.run_id))
    )

    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", f"{config.checkpoint_path}/bronze/wind_turbine_raw")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config.table_name("bronze", "wind_turbine_raw"))
    )
    query.awaitTermination()
