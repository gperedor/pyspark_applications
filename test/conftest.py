from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def test_data_dir():
    return Path(__file__).parent / "data"


@pytest.fixture
def spark(scope="session"):
    spark = (
        SparkSession.builder.config("spark.ui.enabled", "false")
        .appName("test")
        .getOrCreate()
    )
    yield spark
    spark.stop()
