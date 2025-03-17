# Automated BigQuery ETL Pipeline

## Description
A Python-based ETL pipeline that dynamically uploads structured data files to Google BigQuery, with automatic schema inference and user-driven table management.

## Features
- 📂 **Supports multiple file formats** (CSV, JSON, Excel, Parquet)
- 🔍 **Auto-detects schema** from the uploaded file
- 📊 **Creates BigQuery datasets & tables dynamically**
- 🔄 **User choice for table handling** (Append, Recreate, or Cancel)
- 📝 **Interactive file selection** from the local directory
- 🛠 **Robust logging for monitoring errors & progress**

## Directory Structure
```
project_root/
│── data/                     # Directory containing input files
│   ├── sample.csv
│   ├── sample.json
│── logs/                     # Directory for log files
│── etl_script.py             # Main ETL script
│── config.py                 # Configuration file
│── README.md                 # Project documentation
```

## Installation & Setup
1. **Clone the Repository**
   ```bash
   git clone https://github.com/utkrashh/bigquery-etl-pipeline.git
   cd bigquery-etl-pipeline
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Google Cloud Authentication**
   - Ensure you have a Google Cloud project.
   - Authenticate using:
     ```bash
     gcloud auth application-default login
     ```

4. **Update Configuration**
   - Modify `config.py` with your **GCP project ID**, **dataset**, and **table name**.

## Usage
Run the ETL pipeline using:
```bash
python etl_script.py
```

### User Prompts
- **Choose a file** from the available options in the `data/` directory.
- **Handle existing table**: Choose to Append, Recreate, or Cancel the upload.

## Logging
Logs are stored in the `logs/` directory to track execution and errors.

## Future Enhancements
- 🔄 **Google Cloud Storage (GCS) integration** for remote uploads
- 📅 **Scheduling with Apache Airflow or Cloud Functions**
- 📈 **Data validation and transformation steps**

---
**Author:** Your Name  
📧 Contact: utkrashh@gmail.com

