from google.cloud import storage
import os
from dotenv import load_dotenv
import subprocess  # To trigger cleaning separately

# Load environment variables
load_dotenv()

PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_PROCESSED_DESTINATION_BLOB = os.getenv("GCS_PROCESSED_DESTINATION_BLOB")

def upload_to_gcs():

    print("Uploading processed data to Google Cloud Storage...")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_PROCESSED_DESTINATION_BLOB)

    blob.upload_from_filename(PROCESSED_DATA_PATH)

    print(f"File uploaded to gs://{GCS_BUCKET_NAME}/{GCS_PROCESSED_DESTINATION_BLOB}")

if __name__ == "__main__":
    upload_to_gcs()
