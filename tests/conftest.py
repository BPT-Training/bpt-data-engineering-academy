import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    sesion = (
        SparkSession.builder.master("local[1]")
        .appName("bpt-utils-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()
