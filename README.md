# Baby Open Food Facts Data Pipeline Project

## Overview
This project is a **data engineering pipeline** built using **Google Cloud Platform (GCP), Docker, and Kestra** to process and analyze data from the **Open Food Facts dataset**. The final output is a dashboard that visualizes key insights from the dataset about baby foods and their nutritional value.

## Architecture
The project follows a **batch processing** approach and consists of the following steps:

1. **Data Extraction & Storage**
   - Download the Open Food Facts dataset in **Parquet** format.
   - Store the raw data in **Google Cloud Storage (GCS)** (Data Lake).

2. **Data Ingestion to Data Warehouse**
   - Move the data from GCS to **BigQuery** (Data Warehouse).
   - Automate the ingestion process using **Kestra**.

3. **Data Transformation**
   - Perform transformations in **BigQuery** using SQL to clean and prepare data.
   - Store transformed data in a separate BigQuery table/view.

4. **Dashboard & Visualization**
   - Build a dashboard using TBD
   - Include:
     - A **categorical distribution chart** (e.g., distribution of product categories).
     - A **time-series chart** (e.g., number of new products added over time).

## Technologies Used
- **Cloud Platform:** Google Cloud Platform (GCP)
- **Data Storage:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Orchestration:** Kestra
- **Containerization:** Docker
- **Dashboarding:** TBD

---
**Author:** Linda Kaleis

