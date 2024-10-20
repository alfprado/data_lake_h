import logging
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import Row, SparkSession

from scripts.ingestion import add_dt_carga, extract_zip, move_to_history


class TestDataIngestion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = (
            SparkSession.builder.appName("DataIngestionTest")
            .master("local[*]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after tests."""
        cls.spark.stop()

    def setUp(self):
        """Disable logging before each test."""
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        """Re-enable logging after each test."""
        logging.disable(logging.NOTSET)

    def test_add_dt_carga(self):
        """Test that the DT_CARGA column is added correctly."""
        data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
        df = self.spark.createDataFrame(data)
        carga_date = datetime.now().date()

        df_with_carga = add_dt_carga(df, carga_date)

        # Check if the column was added
        self.assertIn("DT_CARGA", df_with_carga.columns)

        # Verify that the column has the correct value
        result = df_with_carga.select("DT_CARGA").collect()[0][0]
        self.assertEqual(result, carga_date)

    @patch("scripts.ingestion.zipfile.ZipFile")
    def test_extract_zip(self, mock_zipfile):
        """Test that the extract_zip function correctly extracts files."""
        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        extract_zip("fake_path.zip", "fake_extract_to")

        mock_zip.extractall.assert_called_once_with("fake_extract_to")

    def test_move_to_history(self):
        """Test that files are correctly moved to the history directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, "source")
            history_dir = os.path.join(temp_dir, "history")

            os.makedirs(source_dir, exist_ok=True)
            with open(os.path.join(source_dir, "test_file.csv"), "w") as f:
                f.write("test")

            move_to_history(source_dir, history_dir)

            # Check if the file was moved to the history directory
            self.assertTrue(os.path.exists(history_dir))
            self.assertTrue(os.path.exists(os.path.join(history_dir, "test_file.csv")))

            # Check if the file was removed from the source directory
            self.assertFalse(os.path.exists(os.path.join(source_dir, "test_file.csv")))


if __name__ == "__main__":
    unittest.main()
