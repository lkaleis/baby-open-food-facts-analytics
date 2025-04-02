import subprocess
import sys
import os
import time


""" 
# List of required packages
required_packages = ['requests', 'google-cloud-storage']

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Check and install required packages
for package in required_packages:
    try:
        __import__(package)
        print(f"{package} is already installed.")
    except ImportError:
        print(f"Installing {package}...")
        install_package(package) """

import requests
from google.cloud import storage

# Initialize GCS client and bucket
client = storage.Client()
BUCKET_NAME = "lk-babyfoodfacts-bucket"
bucket = client.bucket(BUCKET_NAME)

# File URL and local paths
URL = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true"
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(REPO_ROOT, "data")
LOCAL_FILE = os.path.join(DATA_FOLDER, "food.parquet")
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks

# Create 'data' folder if it doesn’t exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

def download_file():
    if os.path.exists(LOCAL_FILE):
        print(f"File already exists: {LOCAL_FILE}. Skipping download.")
        return

    print("Starting download...")
    with requests.get(URL, stream=True) as response:
        response.raise_for_status()
        total_size = int(response.headers.get('Content-Length', 0))
        downloaded_size = 0

        with open(LOCAL_FILE, "wb") as f:
            for chunk in response.iter_content(chunk_size=10_000_000):  # 10MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        progress = min((downloaded_size / total_size) * 100, 100.0)
                        print(f"Downloading: {progress:.2f}% complete", end="\r")

    print(f"\nFile downloaded to: {LOCAL_FILE}")

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(max_retries=3):
    blob_name = os.path.basename(LOCAL_FILE)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(LOCAL_FILE)
            if verify_gcs_upload(blob_name):
                print(f"Uploaded and verified: gs://{BUCKET_NAME}/{blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Upload failed: {e}")
        time.sleep(5)

    print(f"Failed to upload after {max_retries} attempts.")

if __name__ == "__main__":
    download_file()
    upload_to_gcs()
    print("File uploaded to GCS.")