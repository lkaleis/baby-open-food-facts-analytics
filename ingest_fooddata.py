import subprocess
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor


# List of required packages
required_packages = ['requests', 'google-cloud-storage', 'pandas']

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

from google.cloud import storage
import pandas as pd
import requests

# Initialize GCS client
client = storage.Client() 
BUCKET_NAME = "lk-babyfoodfacts-bucket"  # Replace with your GCS bucket\
bucket = client.bucket(BUCKET_NAME)

# File URL and local/GCS paths
URL = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true"
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
DATA_FOLDER = os.path.join(REPO_ROOT, "data")  # Relative path to the 'data' folder
LOCAL_FILE = os.path.join(DATA_FOLDER, "food.parquet")  # Relative path to the 'data' folder
GCS_PATH = "raw-data/food.parquet"

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


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(max_retries=3):
    blob_name = os.path.basename(LOCAL_FILE)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {LOCAL_FILE} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(LOCAL_FILE)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {LOCAL_FILE} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {LOCAL_FILE} after {max_retries} attempts.")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")