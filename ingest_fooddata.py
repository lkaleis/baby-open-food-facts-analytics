import subprocess
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor

# List of required packages
required_packages = ['requests', 'google-cloud-storage', 'pandas', "pyarrow"]

def install_package(package):
    """Installs the package using pip."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Check and install required packages
for package in required_packages:
    try:
        __import__(package)  # Try importing the package
        print(f"{package} is already installed.")
    except ImportError:
        print(f"{package} not found. Installing {package}...")
        install_package(package)

# Now you can proceed with the rest of your script
print("All required packages are installed!")

import pandas as pd
from google.cloud import storage
import requests
import pyarrow.parquet as pq
import pyarrow as pa


# Initialize GCS client
client = storage.Client() 
BUCKET_NAME = "lk-babyfoodfacts-bucket"  # Replace with your GCS bucket
bucket = client.bucket(BUCKET_NAME)

# File URL and local/GCS paths
URL = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true"
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
DATA_FOLDER = os.path.join(REPO_ROOT, "data")  # Relative path to the 'data' folder
LOCAL_FILE = os.path.join(DATA_FOLDER, "food.parquet")  # Relative path to the 'data' folder
FIXED_FILE = os.path.join(DATA_FOLDER, "food_fixed.parquet")  

CHUNK_SIZE = 8 * 1024 * 1024  

# Create 'data' folder if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    print(f"Created 'data' folder at: {DATA_FOLDER}")
else:
    print(f"'data' folder already exists at: {DATA_FOLDER}")

def download_file():
    """Downloads the Parquet file if it doesn't already exist."""
    if os.path.exists(LOCAL_FILE):
        print(f"File already exists: {LOCAL_FILE}. Skipping download.")
        return

    print("Starting download...")

    with requests.get(URL, stream=True) as response:
        response.raise_for_status()

        total_size = int(response.headers.get('Content-Length', 0))
        downloaded_size = 0

        # Download the file in chunks (to handle large files)
        with open(LOCAL_FILE, "wb") as f:
            for chunk in response.iter_content(chunk_size=10_000_000):  # 10MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # Show download progress as a percentage
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        # Ensure progress doesn't exceed 100%
                        progress = min(progress, 100.0)
                        print(f"Downloading: {progress:.2f}% complete", end="\r")

        print(f"\n File downloaded and saved to: {LOCAL_FILE}")

def fix_column_names_streaming(parquet_file, output_file):
    """Fix column names in a Parquet file without loading the entire file into memory."""
    reader = pq.ParquetFile(parquet_file)  # Read Parquet metadata

    # Get actual column names from the first batch of data
    first_batch = next(reader.iter_batches(batch_size=1))
    actual_column_names = first_batch.schema.names  # Extract real column names
    
    # Fix column names: Add _ if the column name starts with a digit
    new_column_names = [f"_{col}" if str(col)[0].isdigit() else col for col in actual_column_names]

    # Create a writer with the updated schema
    arrow_schema = pa.schema([(new_col, first_batch.schema.field(i).type) for i, new_col in enumerate(new_column_names)])
    
    with pq.ParquetWriter(output_file, schema=arrow_schema, use_dictionary=True) as writer:
        for batch in reader.iter_batches(batch_size=100_000):  # Process in chunks
            table = pa.Table.from_batches([batch])
            writer.write_table(table.rename_columns(new_column_names))  # Rename and write

    print(f"Fixed column names. New file saved as: {output_file}")

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(max_retries=3):
    blob_name = os.path.basename(FIXED_FILE)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {FIXED_FILE} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(FIXED_FILE)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {FIXED_FILE} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {FIXED_FILE} after {max_retries} attempts.")

if __name__ == "__main__":
    download_file()

    # Rename columns before uploading to GCS
    fix_column_names_streaming(LOCAL_FILE, FIXED_FILE)

    # Upload to GCS
    upload_to_gcs()

    print("File processed and uploaded to GCS.")