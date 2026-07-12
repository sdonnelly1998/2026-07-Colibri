from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    java_home = os.environ.get("JAVA_HOME")
    java_binary = Path(java_home, "bin", "java") if java_home else None
    if not (java_binary and java_binary.exists()) and not shutil.which("java"):
        pytest.skip("Java 17 is required for local PySpark tests")

    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("colibri-wind-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()
