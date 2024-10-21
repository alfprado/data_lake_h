import logging
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import Row, SparkSession

from ingestion_pipeline import add_dt_carga, extract_zip, move_to_history

@pytest.fixture(scope="module")
def spark():
    """Cria uma sessão Spark para os testes."""
    spark = SparkSession.builder.appName("DataIngestionTest").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(autouse=True)
def disable_logging():
    """Desabilita logs durante os testes."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)

def test_add_dt_carga(spark):
    """Testa se a coluna DT_CARGA é adicionada corretamente."""
    data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
    df = spark.createDataFrame(data)
    carga_date = datetime.now().date()

    df_with_carga = add_dt_carga(df, carga_date)

    # Verifica se a coluna foi adicionada
    assert "DT_CARGA" in df_with_carga.columns

    # Verifica se o valor da coluna é correto
    result = df_with_carga.select("DT_CARGA").collect()[0][0]
    assert result == carga_date

@patch("ingestion_pipeline.zipfile.ZipFile")
def test_extract_zip(mock_zipfile):
    """Testa se a função extract_zip extrai os arquivos corretamente."""
    mock_zip = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip

    extract_zip("fake_path.zip", "fake_extract_to")

    mock_zip.extractall.assert_called_once_with("fake_extract_to")

def test_move_to_history():
    """Testa se os arquivos são movidos corretamente para o diretório de histórico."""
    with tempfile.TemporaryDirectory() as temp_dir:
        source_dir = os.path.join(temp_dir, "source")
        history_dir = os.path.join(temp_dir, "history")

        os.makedirs(source_dir, exist_ok=True)
        with open(os.path.join(source_dir, "test_file.csv"), "w") as f:
            f.write("test")

        move_to_history(source_dir, history_dir)

        # Verifica se o arquivo foi movido para o diretório de histórico
        assert os.path.exists(history_dir)
        assert os.path.exists(os.path.join(history_dir, "test_file.csv"))

        # Verifica se o arquivo foi removido do diretório de origem
        assert not os.path.exists(os.path.join(source_dir, "test_file.csv"))
