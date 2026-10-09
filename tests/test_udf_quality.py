"""Tests de bpt_utils.udf_quality."""

import pytest

from bpt_utils.udf_quality import (
    _udf_apply_expectations,
    _udf_summarize_quality,
)

RULES = [
    {"name": "fare_not_negative", "expression": "fare >= 0"},
    {"name": "origin_not_destination", "expression": "origin <> destination"},
]


@pytest.fixture
def df_bookings(spark):
    return spark.createDataFrame(
        [
            ("R1", "BOG", "MDE", 100.0),  # válida
            ("R2", "BOG", "BOG", 100.0),  # origen = destino
            ("R3", "BOG", "MDE", -5.0),  # tarifa negativa
            ("R4", "CLO", "CLO", None),  # falla ambas: null es incumplida
        ],
        "booking_id string, origin string, destination string, fare double",
    )


def test_splits_valid_and_invalid(df_bookings):
    df_valid, df_invalid = _udf_apply_expectations(df_bookings, RULES)
    assert [row.booking_id for row in df_valid.collect()] == ["R1"]
    invalid_ids = sorted(row.booking_id for row in df_invalid.collect())
    assert invalid_ids == ["R2", "R3", "R4"]
    assert df_valid.columns == df_bookings.columns


def test_invalid_rows_list_failed_rules(df_bookings):
    _, df_invalid = _udf_apply_expectations(df_bookings, RULES)
    failed = {
        row.booking_id: row._failed_rules for row in df_invalid.collect()
    }
    assert failed["R2"] == ["origin_not_destination"]
    assert failed["R4"] == ["fare_not_negative", "origin_not_destination"]
    assert "_quarantine_ts" in df_invalid.columns


def test_summarize_quality(df_bookings):
    _, df_invalid = _udf_apply_expectations(df_bookings, RULES)
    summary = {
        row.rule: row.records
        for row in _udf_summarize_quality(df_invalid).collect()
    }
    assert summary == {"fare_not_negative": 2, "origin_not_destination": 2}


def test_empty_rules_raise(df_bookings):
    with pytest.raises(ValueError):
        _udf_apply_expectations(df_bookings, [])
