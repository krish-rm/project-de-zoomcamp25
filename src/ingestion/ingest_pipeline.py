import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
load_dotenv()

# Paths
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
INGESTED_DATA_PATH = os.getenv("INGESTED_DATA_PATH")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_RAW_DESTINATION_BLOB = "raw/plant_health_data.csv"  # Destination path in GCS for raw data

def upload_to_gcs(file_path, destination_blob_name):
    """Helper function to upload a file to GCS"""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(f"File uploaded to gs://{GCS_BUCKET_NAME}/{destination_blob_name}")

def ingest_data():
    print("Reading raw data...")
    df = pd.read_csv(RAW_DATA_PATH)

    # Save the raw data locally for next steps
    print(f"Saving raw data at: {INGESTED_DATA_PATH}")
    df.to_csv(INGESTED_DATA_PATH, index=False)

    # Upload raw data to Google Cloud Storage (GCS)
    print(f"Uploading raw data to gs://{GCS_BUCKET_NAME}/{GCS_RAW_DESTINATION_BLOB}")
    upload_to_gcs(RAW_DATA_PATH, GCS_RAW_DESTINATION_BLOB)

if __name__ == "__main__":
    ingest_data()
