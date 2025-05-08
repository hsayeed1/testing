import pyodbc
import pandas as pd
from google.cloud import bigquery

# --- 1. SQL Server Connection ---
def fetch_sqlserver_data():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=your_sql_server_host;"      # e.g., "localhost" or "192.168.1.10"
        "DATABASE=your_database;"           # e.g., "my_db"
        "UID=your_username;"
        "PWD=your_password;"
    )

    query = "SELECT name, age, city FROM sqlserver_table"
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    print("✅ Data fetched from SQL Server")
    return df

# --- 2. Upload to BigQuery ---
def upload_to_bigquery(df):
    # Set your GCP credentials and project details
    client = bigquery.Client.from_service_account_json("path/to/your/service_account.json")

    dataset_id = "your_project_id.your_dataset"  # e.g., "my-gcp-project.my_dataset"
    table_id = f"{dataset_id}.sqlserver_table"   # BigQuery table name

    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for upload to complete

    print(f"✅ Uploaded {len(df)} rows to BigQuery table {table_id}")

# --- Run Script ---
if __name__ == "__main__":
    df_data = fetch_sqlserver_data()
    upload_to_bigquery(df_data)