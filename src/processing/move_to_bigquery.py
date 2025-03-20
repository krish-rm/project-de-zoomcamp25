from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define BigQuery parameters
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GCS_URI = os.getenv("GCS_URI")  # gs://bucket/path/to/file.parquet

def load_parquet_to_bq():
    client = bigquery.Client(project=BQ_PROJECT)

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrites the table
    )

    print(f"Loading data from {GCS_URI} into {table_id}...")
    load_job = client.load_table_from_uri(GCS_URI, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Data successfully loaded into {table_id}")

if __name__ == "__main__":
    load_parquet_to_bq()
