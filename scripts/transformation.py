import logging
import os
from datetime import datetime

from pyspark.sql.functions import col, lit


class DataTransformation:
    def __init__(self, **kwargs):
        self.curated_path = kwargs.get("curated_path")
        self.service_path = kwargs.get("service_path")
        self.spark = kwargs.get("spark")

    def transform(self):
        try:
            # Load Parquet files from Curated layer (hot folder)
            df_pacientes = self.spark.read.parquet(
                os.path.join(self.curated_path, "pacientes", "hot")
            )
            df_exames = self.spark.read.parquet(
                os.path.join(self.curated_path, "exames", "hot")
            )

            # Calculate patient age from birth year
            current_year = datetime.now().year
            df_pacientes = df_pacientes.withColumn(
                "VL_IDADE", current_year - col("AA_NASCIMENTO")
            )
            df_pacientes = df_pacientes.drop("AA_NASCIMENTO", "DT_CARGA")
            df_exames = df_exames.drop("DT_CARGA")

            # Join Patients and Exams data
            df_exames_por_pacientes = df_exames.join(
                df_pacientes, on="ID_PACIENTE", how="inner"
            )

            # Save the joined data to the Service layer partitioned by country and state
            df_exames_por_pacientes.write.partitionBy("CD_PAIS", "CD_UF").parquet(
                os.path.join(self.service_path, "exames_por_pacientes"),
                mode="overwrite",
                compression="snappy",
            )

            # Generate filtered dataset for the state of Sao Paulo (CD_UF = 'SP')
            df_exames_por_pacientes_sp = df_exames_por_pacientes.filter(
                col("CD_UF") == lit("SP")
            )
            df_exames_por_pacientes_sp.coalesce(1).write.parquet(
                os.path.join(self.service_path, "exames_por_pacientes_sp"),
                mode="overwrite",
                compression="snappy",
            )

            # Log completion of transformation
            logging.info(
                "Transformation completed: Crossed data saved to the Service layer."
            )
        except Exception as e:
            logging.error(f"Error during data transformation: {e}")
            raise
