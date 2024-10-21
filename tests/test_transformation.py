import logging
import os
import tempfile

import pytest
from pyspark.sql import Row, SparkSession
from transformation import DataTransformation


@pytest.fixture(scope="module")
def spark():
    """Fixture for creating a SparkSession."""
    spark = (
        SparkSession.builder.master("local[*]").appName("TestTransform").getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform_data(spark):
    """Tests the transformation logic for patient and exam data."""

    with tempfile.TemporaryDirectory() as temp_dir:
        curated_path = os.path.join(temp_dir, "curated")
        service_path = os.path.join(temp_dir, "service")

        # Create mock data for patients and exams
        df_pacientes = spark.createDataFrame(
            [
                Row(
                    ID_PACIENTE="1",
                    AA_NASCIMENTO=1980,
                    CD_PAIS="BR",
                    CD_UF="SP",
                ),
                Row(
                    ID_PACIENTE="2",
                    AA_NASCIMENTO=1990,
                    CD_PAIS="BR",
                    CD_UF="RJ",
                ),
            ]
        )
        df_exames = spark.createDataFrame(
            [
                Row(ID_PACIENTE="1", EXAME="Blood Test"),
                Row(ID_PACIENTE="2", EXAME="X-Ray"),
            ]
        )

        # Ensure directories exist
        pacientes_path = os.path.join(curated_path, "pacientes", "hot")
        exames_path = os.path.join(curated_path, "exames", "hot")
        os.makedirs(os.path.dirname(pacientes_path), exist_ok=True)
        os.makedirs(os.path.dirname(exames_path), exist_ok=True)

        # Save mock data as parquet files
        try:
            df_pacientes.write.parquet(pacientes_path, mode="overwrite")
            df_exames.write.parquet(exames_path, mode="overwrite")
        except Exception as e:
            logging.error(f"Error writing parquet files: {e}")
            raise

        # Run the transformation function
        pipeline = DataTransformation(
            curated_path=curated_path,
            service_path=service_path,
            spark=spark,
        )
        try:
            pipeline.transform()
        except Exception as e:
            logging.error(f"Error during transformation: {e}")
            raise

        # Check if the transformed data is saved correctly
        result_path = os.path.join(service_path, "exames_por_pacientes")
        try:
            df_result = spark.read.parquet(result_path)
            assert df_result.count() > 0, "Transformed DataFrame is empty"
            # Additional assertions to validate schema and data
            expected_columns = ["ID_PACIENTE", "EXAME"]
            for column in expected_columns:
                assert column in df_result.columns, f"Missing expected column: {column}"
        except Exception as e:
            logging.error(f"Error reading transformed data: {e}")
            raise
