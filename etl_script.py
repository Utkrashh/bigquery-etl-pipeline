import os
import pandas as pd
import logging
from google.cloud import bigquery
from config import DATA_DIRECTORY, TABLE_ID, LOG_FILE

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_dataset_exists(client, dataset_id):
    """Checks if a BigQuery dataset exists, and creates it if missing."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Change this if needed
        client.create_dataset(dataset)
        logging.info(f"Dataset {dataset_id} created successfully.")

def create_bigquery_table():
    """Ensures the dataset and table exist, creating them if necessary."""
    client = bigquery.Client()
    project_id, dataset_id, table_name = TABLE_ID.split(".")

    # Ensure the dataset exists
    ensure_dataset_exists(client, dataset_id)

    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    ]

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    try:
        client.get_table(table_ref)
        logging.info(f"Table {TABLE_ID} already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Table {TABLE_ID} created successfully.")

def upload_to_bigquery(df, table_id):
    """Uploads a Pandas DataFrame to a specified BigQuery table."""
    if df is None:
        logging.error("Dataframe is None, skipping upload.")
        return
    try:
        client = bigquery.Client()
        create_bigquery_table()  # Ensure dataset and table exist
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        logging.info(f"Data uploaded to {table_id}")
    except Exception as e:
        logging.error(f"Error uploading data to BigQuery: {e}")

def process_files(directory, table_id):
    """Processes all supported files in the given directory and uploads data to BigQuery."""
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        df = None
        try:
            if file_name.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_name.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            elif file_name.endswith('.json'):
                df = pd.read_json(file_path)
            elif file_name.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                logging.warning(f"Unsupported file type: {file_name}")
                continue
            
            upload_to_bigquery(df, table_id)
        except Exception as e:
            logging.error(f"Error processing file {file_name}: {e}")

if __name__ == "__main__":
    logging.info("Starting file processing...")
    process_files(DATA_DIRECTORY, TABLE_ID)
    logging.info("File processing completed.")
