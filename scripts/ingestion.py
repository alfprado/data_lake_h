import os
import zipfile
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging

logging.basicConfig(
    filename='logs/ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def add_dt_carga(df, carga_date):
    """Add DT_CARGA column to DataFrame."""
    return df.withColumn("DT_CARGA", lit(carga_date))

def extract_zip(zip_path, extract_to):
    """Extract files from a zip archive."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logging.info(f"Files successfully extracted from {zip_path} to {extract_to}")
    except FileNotFoundError as e:
        logging.error(f"Zip file not found: {e}")
    except zipfile.BadZipFile as e:
        logging.error(f"Error reading the zip file: {e}")
    except Exception as e:
        logging.error(f"Unknown error during zip extraction: {e}")

def move_to_history(source_dir, history_dir):
    """Move files to the history directory."""
    try:
        if not os.path.exists(history_dir):
            os.makedirs(history_dir)
        for file_name in os.listdir(source_dir):
            full_file_name = os.path.join(source_dir, file_name)
            if os.path.isfile(full_file_name):
                destination_file = os.path.join(history_dir, file_name)
                if os.path.exists(destination_file):
                    os.remove(destination_file)
                shutil.move(full_file_name, history_dir)
        logging.info(f"Files moved to history folder: {history_dir}")
    except FileNotFoundError as e:
        logging.error(f"Error moving files: {e}")
    except Exception as e:
        logging.error(f"Unknown error moving files to history: {e}")

def ingest_data():
    """Main process to handle data ingestion."""
    spark = SparkSession.builder \
        .appName("Data Lake Ingestion") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .getOrCreate()

    today = datetime.now()

    zip_file_path = os.getenv("ZIP_FILE_PATH", "data/source/EINSTEINAgosto.zip")
    stage_path = os.getenv("STAGE_PATH", "data/stage")
    raw_path = os.getenv("RAW_PATH", "data/raw")
    history_path = os.getenv("HISTORY_PATH", f"{stage_path}/history/{today.strftime('%Y%m%d')}")

    extract_zip(zip_file_path, stage_path)

    try:
        csv_files = [f for f in os.listdir(stage_path) if f.endswith('.csv')]
    except FileNotFoundError as e:
        logging.error(f"Directory not found: {e}")
        csv_files = []

    for csv_file in csv_files:
        try:
            csv_file_path = os.path.join(stage_path, csv_file)

            df = spark.read.option("header", "true").option("sep", "|").csv(csv_file_path)

            df = add_dt_carga(df, today.date())

            output_path = os.path.join(raw_path, os.path.splitext(csv_file)[0].split('_')[1].lower())

            df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(output_path)
            logging.info(f"File {csv_file} processed and saved to {output_path}")
        except Exception as e:
            logging.error(f"Error processing file {csv_file}: {e}")

    move_to_history(stage_path, history_path)

    spark.stop()

if __name__ == "__main__":
    ingest_data()
