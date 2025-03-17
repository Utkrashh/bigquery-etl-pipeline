import os

# Directory containing the data files
DATA_DIRECTORY = os.path.join(os.getcwd(), "data")

# BigQuery Table ID in the format `project.dataset.table`
TABLE_ID = "teak-clarity-453916-a5.my_learning_dataset.my_first_table"

# Log file path
LOG_FILE = os.path.join(os.getcwd(), "logs", "etl.log")
