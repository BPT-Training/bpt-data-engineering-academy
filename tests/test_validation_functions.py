"""Tests de bpt_utils.validation_functions y delta_writer."""

import pytest

from bpt_utils.delta_writer import build_merge_condition
from bpt_utils.validation_functions import (
    validate_no_nulls,
    validate_not_empty,
    validate_schema,
    validate_unique_key,
)

SCHEMA = "flight_id string, origin string"


def test_build_merge_condition():
    condition = build_merge_condition(["id", "date"])
    assert condition == "t.id = s.id AND t.date = s.date"


def test_validations_pass_on_clean_data(spark):
    df_clean = spark.createDataFrame([("V1", "BOG"), ("V2", "MDE")], SCHEMA)
    validate_schema(df_clean, {"flight_id", "origin"}, "flights")
    assert validate_not_empty(df_clean, "flights") == 2
    validate_unique_key(df_clean, ["flight_id"], "flights")
    validate_no_nulls(df_clean, ["flight_id", "origin"], "flights")


def test_validate_schema_fails_on_missing_column(spark):
    df_clean = spark.createDataFrame([("V1", "BOG")], SCHEMA)
    with pytest.raises(AssertionError, match="destination"):
        validate_schema(df_clean, {"destination"}, "flights")


def test_validate_not_empty_fails(spark):
    df_empty = spark.createDataFrame([], SCHEMA)
    with pytest.raises(AssertionError, match="vacío"):
        validate_not_empty(df_empty, "flights")


def test_validate_unique_key_fails(spark):
    df_dup = spark.createDataFrame([("V1", "BOG"), ("V1", "MDE")], SCHEMA)
    with pytest.raises(AssertionError, match="duplicada"):
        validate_unique_key(df_dup, ["flight_id"], "flights")


def test_validate_no_nulls_fails(spark):
    df_nulls = spark.createDataFrame([("V1", None)], SCHEMA)
    with pytest.raises(AssertionError, match="origin"):
        validate_no_nulls(df_nulls, ["origin"], "flights")
