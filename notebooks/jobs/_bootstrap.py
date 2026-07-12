# Databricks notebook source
from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


def _project_root() -> Path:
    for candidate in (Path.cwd(), *Path.cwd().parents):
        if (candidate / "src" / "colibri_wind").is_dir():
            return candidate
    raise RuntimeError(f"Could not find project root above {Path.cwd()}")


src_path = str(_project_root() / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.conf.set("spark.sql.session.timeZone", "UTC")
LOGGER = logging.getLogger("colibri_wind")
LOGGER.setLevel(logging.INFO)


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    raw_path: str | None = None
    checkpoint_path: str | None = None
    run_id: str = ""
    total_turbines: int = 15
    turbines_per_file: int = 5
    window_hours: int = 24
    anomaly_sigma: float = 2.0

    def schema_for_layer(self, layer: str) -> str:
        schemas = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }
        try:
            return schemas[layer]
        except KeyError as exc:
            raise ValueError(f"Unknown pipeline layer: {layer}") from exc

    def table_name(self, layer: str, table: str) -> str:
        return f"{self.catalog}.{self.schema_for_layer(layer)}.{table}"

    def quoted_schema(self, layer: str) -> str:
        parts = (self.catalog, self.schema_for_layer(layer))
        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def widget_value(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)


def pipeline_config_from_widgets() -> PipelineConfig:
    return PipelineConfig(
        catalog=widget_value("catalog", "main"),
        bronze_schema=widget_value("bronze_schema", "colibri_bronze_dev"),
        silver_schema=widget_value("silver_schema", "colibri_silver_dev"),
        gold_schema=widget_value("gold_schema", "colibri_gold_dev"),
        raw_path=widget_value("raw_path", "") or None,
        checkpoint_path=widget_value("checkpoint_path", "") or None,
        run_id=widget_value("run_id", "") or uuid.uuid4().hex,
        total_turbines=int(widget_value("total_turbines", "15")),
        turbines_per_file=int(widget_value("turbines_per_file", "5")),
        window_hours=int(widget_value("window_hours", "24")),
        anomaly_sigma=float(widget_value("anomaly_sigma", "2.0")),
    )


def ensure_schemas(config: PipelineConfig) -> None:
    for layer in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.quoted_schema(layer)}")


def overwrite_table(df, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


@contextmanager
def task_event(task_name: str) -> Iterator[None]:
    LOGGER.info("Starting %s", task_name)
    try:
        yield
    except Exception:
        LOGGER.exception("Failed %s", task_name)
        raise
    else:
        LOGGER.info("Finished %s", task_name)
