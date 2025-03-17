import os

# Google Cloud Project ID
PROJECT_ID = "teak-clarity-453916-a5"

# BigQuery Dataset and Table Name
DATASET_ID = "my_learning_dataset"
TABLE_NAME = "my_first_table"

# Construct Full BigQuery Table ID
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# Directory containing the data files
DATA_DIRECTORY = os.path.join(os.getcwd(), "data")

# Log file path
LOG_FILE = os.path.join(os.getcwd(), "logs", "etl.log")
