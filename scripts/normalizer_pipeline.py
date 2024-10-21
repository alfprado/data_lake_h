import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,  trim, lower, regexp_replace, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime


class NormalizerPipeline:
    def __init__(self, **kwargs):
        self.spark = SparkSession.builder \
            .appName("Data Normalization") \
            .master(kwargs.get("spark_master")) \
            .getOrCreate()
        self.raw_path = kwargs.get('raw_path')
        self.curated_path = kwargs.get('curated_path')
        self.rejected_path = kwargs.get('rejected_path')

        self.clean_text_udf = udf(lambda resultado: " ".join(resultado.lower().split()) if resultado else None, StringType())

    def read_data(self, file_path):
        try:
            logging.info(f"Reading raw data from {file_path}...")
            df = self.spark.read.parquet(file_path)
            return df
        except Exception as e:
            logging.error(f"Error reading raw data from {file_path}: {e}")
            raise
    
    # Função de normalização específica para Pacientes
    def normalize_pacientes(self, df):
        df = df.withColumn("CD_MUNICIPIO", regexp_replace(col("CD_MUNICIPIO"), "MMMM", "HIDEN"))
        df = df.withColumn("CD_CEPREDUZIDO", regexp_replace(col("CD_CEPREDUZIDO"), "CCCC", "HIDEN"))

        valid_df = df.dropna(subset=["ID_PACIENTE", "AA_NASCIMENTO", "CD_PAIS", "CD_MUNICIPIO"])
        invalid_df = df.subtract(valid_df)
        
        return valid_df, invalid_df

    # Função de normalização específica para Exames
    def normalize_exames(self, df):
        # df = df.withColumn("DE_ANALITO", col("DE_ANALITO").cast(DateType()))
        clean_col = udf(lambda resultado: " ".join(resultado.lower().split()) if resultado else None, StringType())
        df = df.withColumn("DE_RESULTADO", clean_col(col("DE_RESULTADO")))

        valid_df = df.dropna(subset=["ID_PACIENTE", "DT_COLETA", "DE_ORIGEM", "DE_EXAME", "DE_RESULTADO"])
        invalid_df = df.subtract(valid_df)
        
        return valid_df, invalid_df

    def save_data(self, valid_df, invalid_df, table_name):
        logging.info(f"Saving {table_name} valid and invalid data...")
        try:
            valid_df.coalesce(1).write.mode("overwrite").parquet(os.path.join(self.curated_path, f"{table_name}/hot"))
            invalid_df.coalesce(1).write.mode("overwrite").parquet(os.path.join(self.curated_path, f"{table_name}/rejected"))
        except Exception as e:
            logging.error(f"Error saving {table_name} data: {e}")
            raise


    def process(self):
        try:

            raw_tables = [f for f in os.listdir(self.raw_path) if os.path.isdir(os.path.join(self.raw_path, f))]

            for table in raw_tables:

                logging.info(f"Processing {table} table...")
                df = self.read_data(os.path.join(self.raw_path, table))

                _func = f'normalize_{table}'
                if hasattr(self, _func):
                    normalize = getattr(self, _func)
                    valid_df, invalid_df = normalize(df)
                    self.save_data(valid_df, invalid_df, table)
                else:
                    logging.error(f"There is no normalize for this table: {e}")
                    raise

            logging.info("Data processing completed successfully.")
        except Exception as e:
            logging.error(f"Error in the normalization pipeline: {e}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    today = datetime.now()

    # Leitura das variáveis de ambiente
    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
    }

    # Executa o pipeline de normalização
    try:
        pipeline = NormalizerPipeline(**config)
        pipeline.process()
    except Exception as e:
        logging.error(f"Erro durante a execução do pipeline: {e}")
        raise