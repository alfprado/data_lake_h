import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from scripts.ingestion_pipeline import IngestionPipeline
from scripts.normalizer_pipeline import NormalizerPipeline

load_dotenv()

# Configurar o logging
logging.basicConfig(
    filename='logs/pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    try:
        today = datetime.now()

        data = {
            "spark_master": os.getenv("SPARK_MASTER"),
            "zip_file_path": os.getenv("ZIP_FILE_PATH"),
            "stage_path": os.getenv("STAGE_PATH"),
            "raw_path": os.getenv("RAW_PATH"),
            "curated_path": os.getenv("CURATED_PATH"),
            "history_path": f"{os.getenv('HISTORY_PATH')}{today.strftime('%Y%m%d')}",
        }

        # Run Ingestion
        #ingestion_pipeline = IngestionPipeline(**data)
        #ingestion_pipeline.ingestion_to_raw()

        normalizer_pipeline = NormalizerPipeline(**data)
        normalizer_pipeline.process()

    except Exception as e:
        logging.error(f"Error during execution of the pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
