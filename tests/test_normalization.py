import datetime
import os

import pytest
from normalization import DataNormalization
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DateType, StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    """Creates a Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[*]").appName("TestNormalize").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_env(monkeypatch):
    """Mocks environment variables for tests."""
    monkeypatch.setenv("SPARK_MASTER", "local[*]")
    monkeypatch.setenv("RAW_PATH", "/path/to/raw")
    monkeypatch.setenv("CURATED_PATH", "/path/to/curated")


@pytest.fixture
def df_patient(spark):
    """Creates a mock DataFrame for Patients for testing."""
    data = [
        (
            "123",
            "M",
            "AAAA",
            "BR",
            "MG",
            None,
            "CCCC",
            datetime.date(2023, 10, 20),
        ),
        (
            "124",
            "M",
            "AAAA",
            "BR",
            "MG",
            "Belo Horizonte",
            "CCCC",
            datetime.date(2023, 10, 20),
        ),
        (
            None,
            "F",
            1985,
            "XX",
            "SP",
            "MMMM",
            "05652900",
            datetime.date(2023, 10, 20),
        ),
        ("", "M", None, "", "SP", "MMMM", None, datetime.date(2023, 10, 20)),
        (
            None,
            "F",
            1985,
            "XX",
            "SP",
            "",
            "05652900",
            datetime.date(2023, 10, 20),
        ),
    ]
    schema = StructType(
        [
            StructField("ID_PACIENTE", StringType(), True),
            StructField("IC_SEXO", StringType(), True),
            StructField("AA_NASCIMENTO", StringType(), True),
            StructField("CD_PAIS", StringType(), True),
            StructField("CD_UF", StringType(), True),
            StructField("CD_MUNICIPIO", StringType(), True),
            StructField("CD_CEPREDUZIDO", StringType(), True),
            StructField("DT_CARGA", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def df_exams(spark):
    """Creates a mock DataFrame for Exams for testing."""
    data = [
        (
            "123",
            "2023-10-20",
            "HOSP",
            "Hemograma Contagem Auto",
            "Basófilos #",
            "NOVA  COLETA",
            "µL",
            "0 a 600",
            datetime.date(2023, 10, 20),
        ),
        (
            "",
            "2023-10-20",
            "HOSP",
            "Hemograma Contagem Auto",
            "Basófilos #",
            "NOVA COLETA",
            "µL",
            "0 a 600",
            datetime.date(2023, 10, 20),
        ),
        (
            "124",
            None,
            "HOSP",
            "Hemograma Contagem Auto",
            "Basófilos #",
            "NOVA  COLETA",
            "µL",
            "0 a 600",
            datetime.date(2023, 10, 20),
        ),
        (
            "125",
            "2023-10-20",
            "",
            "Hemograma Contagem Auto",
            "Basófilos #",
            "NOVA  COLETA",
            "µL",
            "0 a 600",
            datetime.date(2023, 10, 20),
        ),
        (
            "126",
            "2023-10-20",
            "HOSP",
            None,
            "Basófilos #",
            "NOVA  COLETA",
            "µL",
            "",
            datetime.date(2023, 10, 20),
        ),
    ]
    schema = StructType(
        [
            StructField("ID_PACIENTE", StringType(), True),
            StructField("DT_COLETA", StringType(), True),
            StructField("DE_ORIGEM", StringType(), True),
            StructField("DE_EXAME", StringType(), True),
            StructField("DE_ANALITO", StringType(), True),
            StructField("DE_RESULTADO", StringType(), True),
            StructField("DE_UNIDADE", StringType(), True),
            StructField("DE_VALOR_REFERENCIA", StringType(), True),
            StructField("DT_CARGA", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def test_normalize_paciente(spark, df_patient):
    """Tests normalization of Patients data."""
    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
    }

    # Execute normalization pipeline
    pipeline = DataNormalization(**config)

    valid_df, invalid_df = pipeline.normalize_pacientes(df_patient)

    # Assert counts for valid and invalid DataFrames
    assert valid_df.count() == 1
    assert invalid_df.count() == 4

    # Verify specific normalizations
    assert valid_df.filter(valid_df["CD_MUNICIPIO"] == "HIDEN").count() == 0
    assert invalid_df.filter(valid_df["CD_CEPREDUZIDO"] == "HIDEN").count() == 1

    # Verify null and empty fields in invalid DataFrame
    assert invalid_df.filter((col("ID_PACIENTE").isNull())).count() == 2
    assert invalid_df.filter((invalid_df["AA_NASCIMENTO"].isNull())).count() == 1
    assert invalid_df.filter((trim(invalid_df["AA_NASCIMENTO"]) == "")).count() == 0
    assert invalid_df.filter((invalid_df["CD_MUNICIPIO"].isNull())).count() == 1
    assert invalid_df.filter((trim(invalid_df["CD_MUNICIPIO"]) == "")).count() == 1
    assert invalid_df.filter(invalid_df["CD_PAIS"].isNull()).count() == 0
    assert invalid_df.filter((trim(invalid_df["CD_PAIS"]) == "")).count() == 1


def test_normalize_exames(spark, df_exams):
    """Tests normalization of Exams data."""
    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
    }

    pipeline = DataNormalization(**config)
    valid_df, invalid_df = pipeline.normalize_exames(df_exams)

    # Assert counts for valid and invalid DataFrames
    assert valid_df.count() == 1
    assert invalid_df.count() == 4

    # Verify that the DataFrame does not contain improperly normalized data
    assert valid_df.filter(col("ID_PACIENTE") == "123").count() == 1
    assert valid_df.filter(col("DE_RESULTADO") == "nova coleta").count() == 1
    assert valid_df.filter(col("DE_RESULTADO") == "nova  coleta").count() == 0
