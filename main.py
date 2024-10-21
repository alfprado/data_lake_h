import logging
import os
from datetime import datetime

from dotenv import load_dotenv

from scripts.ingestion_pipeline import IngestionPipeline
from scripts.normalize_pipeline import NormalizePipeline

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

    config = {
        "spark_master": os.getenv("SPARK_MASTER"),
        "zip_file_path": os.getenv("ZIP_FILE_PATH"),
        "stage_path": os.getenv("STAGE_PATH"),
        "raw_path": os.getenv("RAW_PATH"),
        "curated_path": os.getenv("CURATED_PATH"),
        "history_path": f"{os.getenv('HISTORY_PATH')}-{today.strftime('%Y%m%d')}",
    }

    # Validate required configuration
    missing_vars = [key for key, value in config.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    return config


def run_pipeline():
    try:
        logging.info("Starting pipeline execution...")

        # Get configuration
        config = get_config()

        # Run Ingestion
        logging.info("Starting ingestion pipeline...")
        ingestion_pipeline = IngestionPipeline(**config)
        ingestion_pipeline.ingestion_to_raw()
        logging.info("Ingestion pipeline completed successfully.")

        # Run Normalization
        logging.info("Starting normalization pipeline...")
        normalize_pipeline = NormalizePipeline(**config)
        normalize_pipeline.normalize()
        logging.info("Normalization pipeline completed successfully.")

    except ValueError as ve:
        logging.error(f"Configuration error: {ve}")
    except Exception as e:
        logging.error(f"Error during execution of the pipeline: {e}")
    finally:
        logging.info("Pipeline execution finished.")


if __name__ == "__main__":
    run_pipeline()
