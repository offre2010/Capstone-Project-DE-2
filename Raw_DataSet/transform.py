import pandas as pd
from google.cloud import storage
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

## Write to GCS bucket
def upload_to_gcs(bucket_name, blob_name, file_path, service_account_key):
    try:
        client = storage.Client.from_service_account_json(service_account_key)

        # Get GCS bucket
        bucket = client.get_bucket(bucket_name)

        # Create a blob object in the specified folder within the bucket
        blob = bucket.blob(f'{caps-bucket/Raw_Data}/{filename}')

        # Write file to GCS
        blob.upload_from_filename(file_path)
        logging.info(f"Successfully uploaded {blob_name} to GCS bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error uploading {blob_name} to GCS: {e}")

## Main function to handle CSV processing and upload
def main():
    # Define file paths and GCS details
    local_files = {
        "Raw_AgencyMaster.csv": r"Raw_DataSet\Raw_AgencyMaster.csv",
        "Raw_EmpMaster.csv": r"Raw_DataSet\Raw_EmpMaster.csv",
        "Raw_nycpayroll_2020.csv": r"Raw_DataSet\Raw_nycpayroll_2020.csv",
        "Raw_nycpayroll_2021.csv": r"Raw_DataSet\Raw_nycpayroll_2021.csv",
        "Raw_TitleMaster.csv": r"Raw_DataSet\Raw_TitleMaster.csv",
    }

    bucket_name = "caps-bucket"
    service_account_key = os.getenv("SERVICE_ACCOUNT_KEY_PATH", "service_account_key.json")

    # Upload each file to GCS
    for blob_name, file_path in local_files.items():
        if os.path.exists(file_path):
            upload_to_gcs(bucket_name, 'caps-bucket/Raw_Data', file_path, service_account_key)
        else:
            logging.error(f"File {file_path} does not exist.")

if __name__ == "__main__":
    main()
