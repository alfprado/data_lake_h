import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from scripts.ingestion import DataIngestion
from scripts.normalization import DataNormalization
from scripts.transformation import DataTransformation

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_config():
    """Fetch configuration from environment variables."""
    today = datetime.now()

    spark = (
        SparkSession.builder.appName("Data Pepeline")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .master(os.getenv("SPARK_MASTER"))
        .getOrCreate()
    )

    config = {
        "spark": spark,
        "zip_file_path": os.getenv("ZIP_FILE_PATH"),
        "stage_path": os.getenv("STAGE_PATH"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
        "service_path": os.getenv("SERVICE_PATH"),
        "history_path": f"{os.getenv('HISTORY_PATH')}-{today.strftime('%Y%m%d')}",
    }

    # Validate required configuration
    missing_vars = [key for key, value in config.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    return config


def run():
    try:
        logging.info("Starting pipeline execution...")

        # Get configuration
        config = get_config()

        # Run Ingestion
        logging.info("Starting ingestion pipeline...")
        ingestion = DataIngestion(**config)
        ingestion.ingestion_to_raw()
        logging.info("Ingestion pipeline completed successfully.")

        # Run Normalization
        logging.info("Starting normalization pipeline...")
        normalize = DataNormalization(**config)
        normalize.normalize()
        logging.info("Normalization pipeline completed successfully.")

        # Run Transform
        logging.info("Starting trasnform pipeline...")
        transform = DataTransformation(**config)
        transform.transform()
        logging.info("Transform pipeline completed successfully.")

    except ValueError as ve:
        logging.error(f"Configuration error: {ve}")
    except Exception as e:
        logging.error(f"Error during execution of the pipeline: {e}")
    finally:
        logging.info("Pipeline execution finished.")


if __name__ == "__main__":
    run()
