import logging
import os

from pyspark.sql.functions import col, regexp_replace, udf
from pyspark.sql.types import StringType


class DataNormalization:
    def __init__(self, **kwargs):
        self.spark = kwargs.get("spark")
        self.raw_path = kwargs.get("raw_path")
        self.curated_path = kwargs.get("curated_path")
        self.rejected_path = kwargs.get("rejected_path")

        self.clean_text_udf = udf(
            lambda resultado: (
                " ".join(resultado.lower().split()) if resultado else None
            ),
            StringType(),
        )

    def read_data(self, file_path):
        try:
            logging.info(f"Reading raw data from {file_path}...")
            df = self.spark.read.parquet(file_path)
            return df
        except Exception as e:
            logging.error(f"Error reading raw data from {file_path}: {e}")
            raise

    def normalize_pacientes(self, df):
        # Replace 'MMMM' and 'CCCC' in columns
        df = df.withColumn(
            "CD_MUNICIPIO",
            regexp_replace(col("CD_MUNICIPIO"), "MMMM", "HIDEN"),
        )
        df = df.withColumn(
            "CD_CEPREDUZIDO",
            regexp_replace(col("CD_CEPREDUZIDO"), "CCCC", "HIDEN"),
        )

        valid_df = df.filter(
            (df["ID_PACIENTE"].isNotNull() & (df["ID_PACIENTE"] != ""))
            & (df["AA_NASCIMENTO"].isNotNull() & (df["AA_NASCIMENTO"] != ""))
            & (df["CD_PAIS"].isNotNull() & (df["CD_PAIS"] != ""))
            & (df["CD_MUNICIPIO"].isNotNull() & (df["CD_MUNICIPIO"] != ""))
        )

        invalid_df = df.subtract(valid_df)

        return valid_df, invalid_df

    def normalize_exames(self, df):
        # Clean 'DE_RESULTADO' using the defined UDF
        df = df.withColumn("DE_RESULTADO", self.clean_text_udf(col("DE_RESULTADO")))

        valid_df = df.filter(
            (df["ID_PACIENTE"].isNotNull() & (df["ID_PACIENTE"] != ""))
            & (
                df["DT_COLETA"].isNotNull()
                & (df["DT_COLETA"] != "")
                & (df["DE_ORIGEM"].isNotNull() & (df["DE_ORIGEM"] != ""))
                & (df["DE_EXAME"].isNotNull() & (df["DE_EXAME"] != ""))
                & (df["DE_RESULTADO"].isNotNull() & (df["DE_RESULTADO"] != ""))
            )
        )

        # Get the invalid rows by subtracting the valid DataFrame from the original
        invalid_df = df.subtract(valid_df)

        return valid_df, invalid_df

    def save_data(self, valid_df, invalid_df, table_name):
        logging.info(f"Saving {table_name} valid and invalid data...")
        try:
            valid_df.coalesce(1).write.mode("overwrite").parquet(
                os.path.join(self.curated_path, f"{table_name}/hot")
            )
            invalid_df.coalesce(1).write.mode("overwrite").parquet(
                os.path.join(self.curated_path, f"{table_name}/rejected")
            )
        except Exception as e:
            logging.error(f"Error saving {table_name} data: {e}")
            raise

    def normalize(self):
        try:
            # List tables in the raw data directory
            raw_tables = [
                f
                for f in os.listdir(self.raw_path)
                if os.path.isdir(os.path.join(self.raw_path, f))
            ]

            for table in raw_tables:
                logging.info(f"Processing {table} table...")
                df = self.read_data(os.path.join(self.raw_path, table))

                # Call the corresponding normalization function
                _func = f"normalize_{table}"
                if hasattr(self, _func):
                    normalize = getattr(self, _func)
                    valid_df, invalid_df = normalize(df)
                    self.save_data(valid_df, invalid_df, table)
                else:
                    logging.error(f"No normalization function found for table: {table}")
                    raise AttributeError(
                        f"No normalization function found for table: {table}"
                    )

            logging.info("Data processing completed successfully.")
        except Exception as e:
            logging.error(f"Error in the normalization pipeline: {e}")
            raise
