import logging
import os
import shutil
import zipfile
from datetime import datetime

from pyspark.sql.functions import lit


def add_dt_carga(df, carga_date):
    """Add DT_CARGA column to DataFrame."""
    return df.withColumn("DT_CARGA", lit(carga_date))


def extract_zip(zip_path, extract_to):
    """Extract files from a zip archive."""
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def move_to_history(source_dir, history_dir):
    """Move files to the history directory."""
    if not os.path.exists(history_dir):
        os.makedirs(history_dir)
    for file_name in os.listdir(source_dir):
        full_file_name = os.path.join(source_dir, file_name)
        if os.path.isfile(full_file_name):
            destination_file = os.path.join(history_dir, file_name)
            if os.path.exists(destination_file):
                os.remove(destination_file)
            shutil.move(full_file_name, history_dir)


class DataIngestion:
    def __init__(self, **kwargs):
        self.spark = kwargs.get("spark")
        self.stage_path = kwargs.get("stage_path")
        self.raw_path = kwargs.get("raw_path")
        self.history_path = kwargs.get("history_path")
        self.zip_file_path = kwargs.get("zip_file_path")

    def ingestion_to_raw(self):
        try:
            logging.info("Starting file extraction...")
            extract_zip(self.zip_file_path, self.stage_path)

            csv_files = [f for f in os.listdir(self.stage_path) if f.endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError("No CSV files found in the stage folder.")

            for csv_file in csv_files:
                csv_file_path = os.path.join(self.stage_path, csv_file)

                df = (
                    self.spark.read.option("header", "true")
                    .option("sep", "|")
                    .csv(csv_file_path)
                )

                df = add_dt_carga(df, datetime.now().date())

                output_path = os.path.join(
                    self.raw_path,
                    os.path.splitext(csv_file)[0].split("_")[1].lower(),
                )

                df.coalesce(1).write.mode("overwrite").option(
                    "compression", "snappy"
                ).parquet(output_path)

            logging.info("Moving files to history...")
            move_to_history(self.stage_path, self.history_path)

        except Exception as e:
            logging.error(f"Error during data ingestion: {e}")
            raise
