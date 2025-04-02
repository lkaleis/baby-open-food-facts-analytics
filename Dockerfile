# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy the script into the container
COPY ingest_foodfactsdata.py .

# Install dependencies
RUN pip install --no-cache-dir requests google-cloud-storage

# Set environment variable for GCS authentication (if using a key file)
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/gcs-key.json

# Run the script when the container launches
CMD ["python", "ingest_foodfactsdata.py"]