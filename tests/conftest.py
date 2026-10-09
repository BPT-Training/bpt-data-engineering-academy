"""Fixtures compartidas de pytest: sesión local de Spark."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Sesión de Spark local con una sola partición de shuffle."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("bpt-utils-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
