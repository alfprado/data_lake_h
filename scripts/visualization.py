import argparse
import logging
import os

import matplotlib.pyplot as plt
import pandas as pd


def setup_logging():
    """Sets up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )


def load_data(service_path):
    """Loads the parquet data from the specified path.

    Args:
        service_path (str): Path to the service directory containing parquet files.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    file_path = os.path.join(service_path, "exames_por_pacientes_sp")
    try:
        df = pd.read_parquet(file_path)
        logging.info(f"Data loaded successfully from {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def filter_qualitative_results(df):
    """Filters the DataFrame for records with non-numeric results.

    Args:
        df (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: Filtered DataFrame with only qualitative results.
    """
    filtered_df = df[
        df["DE_RESULTADO"].str.contains(
            r"^[a-zA-Z\sáéíóúãõç\-+*~]+$", regex=True, na=False
        )
    ]
    logging.info("Filtered qualitative results successfully.")
    return filtered_df


def plot_histogram(df_qualitativo):
    """Generates and saves a histogram showing the count of records by 'DE_ANALITO'.

    Args:
        df_qualitativo (pd.DataFrame): Filtered DataFrame with qualitative results.
    """
    df_analito_counts = df_qualitativo["DE_ANALITO"].value_counts(ascending=False)
    plt.figure(figsize=(20, 10))
    df_analito_counts.plot(kind="bar", color="skyblue")
    plt.xlabel("DE_ANALITO", fontsize=14)
    plt.ylabel("Record Count", fontsize=14)
    plt.title("Record Count by DE_ANALITO (Qualitative Values)", fontsize=16)
    plt.xticks(rotation=75, ha="right", fontsize=10)
    plt.yticks(fontsize=12)
    plt.tight_layout()
    output_path = "output/histogram_updated.png"
    plt.savefig(output_path)
    logging.info(f"Histogram saved to {output_path}")


def main(service_path):
    """Main function to execute the data loading, filtering, and plotting.

    Args:
        service_path (str): Path to the service directory containing parquet files.
    """
    df = load_data(service_path)
    df_qualitativo = filter_qualitative_results(df)
    plot_histogram(df_qualitativo)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Generate histogram from parquet data")
    parser.add_argument(
        "--service-path",
        required=True,
        help="Path to the service directory containing parquet files",
    )
    args = parser.parse_args()

    try:
        main(args.service_path)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
