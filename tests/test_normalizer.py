import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from normalizer_pipeline import NormalizerPipeline
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import trim

import datetime
import os

@pytest.fixture(scope="module")
def spark():
    """Cria uma sessão Spark para os testes."""
    spark = SparkSession.builder.master("local[*]").appName("TestProcessamento").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_env(monkeypatch):
    """Simula as variáveis de ambiente para os testes."""
    monkeypatch.setenv("SPARK_MASTER", "local[*]")
    monkeypatch.setenv("RAW_PATH", "/path/to/raw")
    monkeypatch.setenv("CURATED_PATH", "/path/to/curated")

@pytest.fixture
def df_pacientes(spark):
    """Cria um DataFrame simulado de Pacientes para testes."""
    data = [
        ("123", "M", "AAAA", "BR", "MG", None, "CCCC", datetime.date(2023, 10, 20)),
        ("124", "M", "AAAA", "BR", "MG", "Belo Horizonte", "CCCC", datetime.date(2023, 10, 20)),
        (None, "F", 1985, "XX", "SP", "MMMM", "05652900", datetime.date(2023, 10, 20)),
        ("", "M", None, "", "SP", "MMMM", None, datetime.date(2023, 10, 20)),
        (None, "F", 1985, "XX", "SP", "", "05652900", datetime.date(2023, 10, 20)),
    ]
    schema = StructType([
        StructField("ID_PACIENTE", StringType(), True),
        StructField("IC_SEXO", StringType(), True),
        StructField("AA_NASCIMENTO", StringType(), True),
        StructField("CD_PAIS", StringType(), True),
        StructField("CD_UF", StringType(), True),
        StructField("CD_MUNICIPIO", StringType(), True),
        StructField("CD_CEPREDUZIDO", StringType(), True),
        StructField("DT_CARGA", DateType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def df_exames(spark):
    """Cria um DataFrame simulado de Exames para testes."""
    data = [
        ("123", datetime.date(2023,10,20), "HOSP", "Hemograma Contagem Auto", "Basófilos #", "NOVA  COLETA", "µL", "0 a 600", datetime.date(2023, 10, 20)),
        ("", datetime.date(2023,10,20), "HOSP", "Hemograma Contagem Auto", "Basófilos #", "NOVA COLETA", "µL", "0 a 600", datetime.date(2023, 10, 20)),
        ("124", None, "HOSP", "Hemograma Contagem Auto", "Basófilos #", "NOVA  COLETA", "µL", "0 a 600", datetime.date(2023, 10, 20)),
        ("125", datetime.date(2023,10,20), "", "Hemograma Contagem Auto", "Basófilos #", "NOVA  COLETA", "µL", "0 a 600", datetime.date(2023, 10, 20)),
        ("126", datetime.date(2023,10,20), "HOSP", None, "Basófilos #", "NOVA  COLETA", "µL", "", datetime.date(2023, 10, 20))
    ]
    schema = StructType([
        StructField("ID_PACIENTE", StringType(), True),
        StructField("DT_COLETA", DateType(), True),
        StructField("DE_ORIGEM", StringType(), True),
        StructField("DE_EXAME", StringType(), True),
        StructField("DE_ANALITO", StringType(), True),
        StructField("DE_RESULTADO", StringType(), True),
        StructField("DE_UNIDADE", StringType(), True),
        StructField("DE_VALOR_REFERENCIA", StringType(), True),
        StructField("DT_CARGA", DateType(), True)
    ])

    return spark.createDataFrame(data, schema)


def test_normalize_pacientes(spark, df_pacientes):
    """Testa a normalização dos dados de Pacientes."""
    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
    }

    # Executa o pipeline de normalização
    
    pipeline = NormalizerPipeline(**config)
    
    valid_df, invalid_df = pipeline.normalize_pacientes(df_pacientes)

    assert valid_df.count() == 1
    assert invalid_df.count() == 4

    # Verificar se 'MMMM' e 'CCCC' foram normalizados corretamente
    assert valid_df.filter(valid_df["CD_MUNICIPIO"] == "HIDEN").count() == 0
    assert invalid_df.filter(valid_df["CD_CEPREDUZIDO"] == "HIDEN").count() == 1

    assert invalid_df.filter((col("ID_PACIENTE").isNull())).count() == 2
    
    assert invalid_df.filter((invalid_df["AA_NASCIMENTO"].isNull())).count() == 1
    assert invalid_df.filter((trim(invalid_df["AA_NASCIMENTO"]) == "")).count() == 0

    assert invalid_df.filter((invalid_df["CD_MUNICIPIO"].isNull())).count() == 1
    assert invalid_df.filter((trim(invalid_df["CD_MUNICIPIO"]) == "")).count() == 1

    assert invalid_df.filter(invalid_df["CD_PAIS"].isNull()).count() == 0
    assert invalid_df.filter((trim(invalid_df["CD_PAIS"]) == "")).count() == 1


def test_normalize_exames(spark, df_exames):
    """Testa a normalização dos dados de Exames."""
    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
    }

    # Executa o pipeline de normalização
    pipeline = NormalizerPipeline(**config)
    valid_df, invalid_df = pipeline.normalize_exames(df_exames)


    # Verificar se 'DE_RESULTADO' foi convertido para minúsculas e ajustado
    assert valid_df.filter(valid_df["DE_RESULTADO"] == "nova coleta").count() == 3

    assert invalid_df.filter(valid_df["DE_RESULTADO"] == "nova  coleta").count() == 0


