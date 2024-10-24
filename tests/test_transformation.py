import logging
import os
import tempfile
from datetime import date

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
                    IC_SEXO="F",
                    AA_NASCIMENTO="1980",
                    CD_UF="SP",
                    CD_MUNICIPIO="SÃO PAULO",
                    CD_CEPREDUZIDO="HIDEN",
                    CD_PAIS="BR",
                    DT_CARGA="2024-10-22",
                ),
                Row(
                    ID_PACIENTE="2",
                    IC_SEXO="M",
                    AA_NASCIMENTO="1950",
                    CD_UF="RJ",
                    CD_MUNICIPIO="TRINDADE",
                    CD_CEPREDUZIDO="HIDEN",
                    CD_PAIS="BR",
                    DT_CARGA="2024-10-22",
                ),
            ]
        )
        df_exames = spark.createDataFrame(
            [
                Row(
                    ID_PACIENTE="2",
                    DT_COLETA="04/06/2020",
                    DE_ORIGEM="HOSP",
                    DE_EXAME="Dosagem de Sódio",
                    DE_ANALITO="Sódio",
                    DE_RESULTADO="134",
                    CD_UNIDADE="mEq/L",
                    DE_VALOR_REFERENCIA="135 a 145",
                    DT_CARGA=date(2023, 10, 20),
                ),
                Row(
                    ID_PACIENTE="1",
                    DT_COLETA="04/06/202",
                    DE_ORIGEM="HOSP",
                    DE_EXAME="Dosagem de Uréia",
                    DE_ANALITO="Uréia",
                    DE_RESULTADO="24",
                    CD_UNIDADE="mg/dL",
                    DE_VALOR_REFERENCIA="17 a 49",
                    DT_CARGA=date(2023, 10, 20),
                ),
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
            expected_columns = [
                "ID_PACIENTE",
                "IC_SEXO",
                "CD_UF",
                "CD_MUNICIPIO",
                "CD_CEPREDUZIDO",
                "CD_PAIS",
                "VL_IDADE",
                "DT_COLETA",
                "DE_ORIGEM",
                "DE_EXAME",
                "DE_ANALITO",
                "DE_RESULTADO",
                "CD_UNIDADE",
                "DE_VALOR_REFERENCIA",
            ]
            for column in expected_columns:
                assert column in df_result.columns, f"Missing expected column: {column}"
        except Exception as e:
            logging.error(f"Error reading transformed data: {e}")
            raise
