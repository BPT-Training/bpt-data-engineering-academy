"""Tests de bpt_utils.udf_cleaning."""

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from bpt_utils.udf_cleaning import (
    _udf_clean_columns,
    _udf_drop_duplicates_by_key,
    _udf_normalize_column_names,
    parse_timestamp,
    to_snake_case,
)


@pytest.mark.parametrize(
    ("raw_name", "expected"),
    [
        ("Código IATA", "codigo_iata"),
        ("flightId", "flight_id"),
        ("  Fecha-Salida ", "fecha_salida"),
        ("already_snake", "already_snake"),
    ],
)
def test_to_snake_case(raw_name, expected):
    assert to_snake_case(raw_name) == expected


def test_normalize_column_names_keeps_technical_columns(spark):
    df_raw = spark.createDataFrame(
        [(1, "a", "x")], ["Flight Id", "Código Origen", "_rescued_data"]
    )
    df_clean = _udf_normalize_column_names(df_raw)
    assert df_clean.columns == ["flight_id", "codigo_origen", "_rescued_data"]


def test_clean_columns_converts_blanks_to_null(spark):
    df_raw = spark.createDataFrame(
        [(" bog ", 1), ("   ", 2)], "origin string, n int"
    )
    df_expected = spark.createDataFrame(
        [("bog", 1), (None, 2)], "origin string, n int"
    )
    assertDataFrameEqual(_udf_clean_columns(df_raw), df_expected)


def test_drop_duplicates_by_key_keeps_latest(spark):
    schema = "flight_id string, version int, file string"
    df_raw = spark.createDataFrame(
        [("V1", 1, "a.csv"), ("V1", 2, "b.csv"), ("V2", 1, "a.csv")], schema
    )
    df_dedup = _udf_drop_duplicates_by_key(
        df_raw, ["flight_id"], [F.col("version").desc(), F.col("file").desc()]
    )
    df_expected = spark.createDataFrame(
        [("V1", 2, "b.csv"), ("V2", 1, "a.csv")], schema
    )
    assertDataFrameEqual(df_dedup, df_expected)


def test_parse_timestamp_accepts_several_formats(spark):
    df_raw = spark.createDataFrame(
        [("2026-10-14 08:30",), ("14/10/2026 08:30",), ("basura",)],
        "raw_ts string",
    )
    df_parsed = df_raw.select(
        parse_timestamp(
            "raw_ts", ["yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm"]
        ).alias("ts")
    )
    values = [row.ts for row in df_parsed.collect()]
    expected_ts = datetime(2026, 10, 14, 8, 30)
    assert values == [expected_ts, expected_ts, None]
