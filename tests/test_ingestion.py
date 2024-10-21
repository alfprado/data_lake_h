import logging
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from ingestion import add_dt_carga, extract_zip, move_to_history
from pyspark.sql import Row, SparkSession


@pytest.fixture(scope="module")
def spark():
    """Creates a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("DataIngestionTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def disable_logging():
    """Disables logging during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def test_add_dt_carga(spark):
    """Tests if the DT_CARGA column is added correctly."""
    data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
    df = spark.createDataFrame(data)
    carga_date = datetime.now().date()

    df_with_carga = add_dt_carga(df, carga_date)

    # Check if the column was added
    assert "DT_CARGA" in df_with_carga.columns

    # Check if the value of the column is correct
    result = df_with_carga.select("DT_CARGA").collect()[0][0]
    assert result == carga_date


@patch("ingestion.zipfile.ZipFile")
def test_extract_zip(mock_zipfile):
    """Tests if the extract_zip function extracts files correctly."""
    mock_zip = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip

    extract_zip("fake_path.zip", "fake_extract_to")

    mock_zip.extractall.assert_called_once_with("fake_extract_to")


def test_move_to_history():
    """Tests if files are moved correctly to the history directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        source_dir = os.path.join(temp_dir, "source")
        history_dir = os.path.join(temp_dir, "history")

        os.makedirs(source_dir, exist_ok=True)
        with open(os.path.join(source_dir, "test_file.csv"), "w") as f:
            f.write("test")

        move_to_history(source_dir, history_dir)

        # Check if the file was moved to the history directory
        assert os.path.exists(history_dir)
        assert os.path.exists(os.path.join(history_dir, "test_file.csv"))

        # Check if the file was removed from the source directory
        assert not os.path.exists(os.path.join(source_dir, "test_file.csv"))
