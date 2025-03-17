import os
import pandas as pd
import logging
from google.cloud import bigquery
from config import DATA_DIRECTORY, TABLE_ID, PROJECT_ID, DATASET_ID, TABLE_NAME, LOG_FILE

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def infer_bq_schema(df):
    """Infers BigQuery schema from a Pandas DataFrame."""
    type_mapping = {
        "int64": "INTEGER",
        "float64": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "STRING",
    }

    schema = []
    for col, dtype in df.dtypes.items():
        bq_type = type_mapping.get(str(dtype), "STRING")  # Default to STRING if type is unknown
        schema.append(bigquery.SchemaField(col, bq_type, mode="NULLABLE"))

    return schema

def ensure_dataset_exists(client):
    """Ensures the BigQuery dataset exists."""
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logging.info(f"Dataset {DATASET_ID} created successfully.")

def handle_existing_table(client, table_ref):
    """Checks if table exists and prompts user for action."""
    try:
        client.get_table(table_ref)  # Check if table exists
        logging.info(f"Table {TABLE_ID} already exists.")

        # Ask the user what to do
        action = input("Table already exists. Choose an option:\n"
                       "1. Append new rows to existing table\n"
                       "2. Delete and recreate table\n"
                       "3. Cancel upload\n"
                       "Enter your choice (1/2/3): ")

        if action == "2":
            client.delete_table(table_ref)
            logging.info(f"Table {TABLE_ID} deleted.")
            return "RECREATE"
        elif action == "1":
            return "APPEND"
        else:
            logging.info("Operation cancelled by user.")
            return "CANCEL"
    except Exception:
        return "NEW"

def create_or_update_table(df):
    """Creates or recreates a BigQuery table based on user choice."""
    client = bigquery.Client()
    ensure_dataset_exists(client)

    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_NAME)

    action = handle_existing_table(client, table_ref)

    if action == "CANCEL":
        return False
    elif action == "RECREATE":
        schema = infer_bq_schema(df)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Table {TABLE_ID} created successfully with inferred schema.")

    return True

def upload_to_bigquery(df):
    """Uploads a Pandas DataFrame to BigQuery based on user choice."""
    if df is None:
        logging.error("Dataframe is None, skipping upload.")
        return

    try:
        client = bigquery.Client()
        if not create_or_update_table(df):
            return  # Exit if user cancels

        job = client.load_table_from_dataframe(df, TABLE_ID)
        job.result()
        logging.info(f"Data uploaded to {TABLE_ID}")
    except Exception as e:
        logging.error(f"Error uploading data to BigQuery: {e}")

def process_file():
    """Asks user for a file to upload and processes it."""
    files = [f for f in os.listdir(DATA_DIRECTORY) if f.endswith(('.csv', '.xlsx', '.json', '.parquet'))]

    if not files:
        print("No supported files found in the data directory.")
        return

    print("Available files:")
    for i, file in enumerate(files, start=1):
        print(f"{i}. {file}")

    choice = input("Enter the number of the file you want to upload: ")
    
    try:
        choice = int(choice)
        if 1 <= choice <= len(files):
            file_name = files[choice - 1]
        else:
            print("Invalid choice.")
            return
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    file_path = os.path.join(DATA_DIRECTORY, file_name)

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
            print("Unsupported file format.")
            return
        
        upload_to_bigquery(df)
    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")

if __name__ == "__main__":
    logging.info("Starting file processing...")
    process_file()
    logging.info("File processing completed.")
