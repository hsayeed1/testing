from simple_salesforce import Salesforce
from google.cloud import bigquery
import pandas as pd


# --- 1. Fetch Data from Salesforce ---
def fetch_salesforce_data():
    # Connect to Salesforce
    sf = Salesforce(
        username='your_username',
        password='your_password',
        security_token='your_security_token'
    )

    # Query data
    soql = "SELECT name, age__c, city__c FROM Contact"  # assuming age and city are custom fields
    records = sf.query_all(soql)['records']

    # Convert to DataFrame
    df = pd.DataFrame(records)
    df = df[['name', 'age__c', 'city__c']]
    df.columns = ['name', 'age', 'city']  # rename columns to match BigQuery
    print("✅ Data fetched from Salesforce")
    return df


# --- 2. Upload to BigQuery ---
def upload_to_bigquery(df):
    client = bigquery.Client.from_service_account_json("path/to/service_account.json")

    table_id = "your_project_id.your_dataset.salesforce_data"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"✅ Uploaded {len(df)} records to BigQuery table: {table_id}")


# --- Run Script ---
if __name__ == "__main__":
    data = fetch_salesforce_data()
    upload_to_bigquery(data)