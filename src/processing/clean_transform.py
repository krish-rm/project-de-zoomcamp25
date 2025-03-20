import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INGESTED_DATA_PATH = os.getenv("INGESTED_DATA_PATH")  # Read from ingestion output
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")

def clean_data():
    print("Reading ingested raw data...")
    df = pd.read_csv(INGESTED_DATA_PATH)  # Read the saved raw CSV

    # Convert Timestamp to datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])

    # Check unique values in Plant_Health_Status
    print("Unique Plant_Health_Status values:", df["Plant_Health_Status"].unique())

    # Summary after changes
    print(df.info())

    print(f"Saving cleaned data at: {PROCESSED_DATA_PATH}")
    df.to_parquet(PROCESSED_DATA_PATH, index=False)  # Save processed data

if __name__ == "__main__":
    clean_data()

