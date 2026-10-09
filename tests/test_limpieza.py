from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from bpt_utils.limpieza import (
    a_snake_case,
    deduplicar,
    normalizar_columnas,
    parsear_timestamp,
    recortar_textos,
)


@pytest.mark.parametrize(
    "entrada, esperado",
    [
        ("Código IATA", "codigo_iata"),
        ("vueloId", "vuelo_id"),
        ("  Fecha-Salida ", "fecha_salida"),
        ("ya_en_snake", "ya_en_snake"),
    ],
)
def test_a_snake_case(entrada, esperado):
    assert a_snake_case(entrada) == esperado


def test_normalizar_columnas(spark):
    df = spark.createDataFrame([(1, "a", "x")], ["Vuelo Id", "Código Origen", "_rescued_data"])
    assert normalizar_columnas(df).columns == ["vuelo_id", "codigo_origen", "_rescued_data"]


def test_recortar_textos_convierte_vacios_en_null(spark):
    df = spark.createDataFrame([(" bog ", 1), ("   ", 2)], "origen string, n int")
    esperado = spark.createDataFrame([("bog", 1), (None, 2)], "origen string, n int")
    assertDataFrameEqual(recortar_textos(df), esperado)


def test_deduplicar_se_queda_con_el_mas_reciente(spark):
    df = spark.createDataFrame(
        [("V1", 1, "a.csv"), ("V1", 2, "b.csv"), ("V2", 1, "a.csv")],
        "vuelo_id string, version int, archivo string",
    )
    resultado = deduplicar(df, ["vuelo_id"], [F.col("version").desc(), F.col("archivo").desc()])
    esperado = spark.createDataFrame(
        [("V1", 2, "b.csv"), ("V2", 1, "a.csv")], "vuelo_id string, version int, archivo string"
    )
    assertDataFrameEqual(resultado, esperado)


def test_parsear_timestamp_acepta_varios_formatos(spark):
    df = spark.createDataFrame([("2026-10-14 08:30",), ("14/10/2026 08:30",), ("basura",)], "t string")
    resultado = df.select(parsear_timestamp("t", ["yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm"]).alias("ts"))
    valores = [r.ts for r in resultado.collect()]
    assert valores == [datetime(2026, 10, 14, 8, 30), datetime(2026, 10, 14, 8, 30), None]
