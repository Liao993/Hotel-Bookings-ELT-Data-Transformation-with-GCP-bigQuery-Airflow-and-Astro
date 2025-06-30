# Hotel Bookings Data Transformation with GCP, Airflow, and Astro

This project demonstrates how to build an ELT (Extract, Load, Transform) data pipeline to process and transform hotel booking data containing over 100,000 records using Google Cloud Platform (GCP), Apache Airflow, and Astronomer Astro. The pipeline extracts data from Google Cloud Storage (GCS), loads it into BigQuery, transforms it into dimensional and fact tables using dbt and Cosmos, and ensures data quality with Soda, creating a robust analytics-ready dataset for the hospitality sector.

## Features

- Extract CSV data from GCS and load it into a BigQuery staging table.
- Transform raw data into dimensional tables (e.g., `dim_date`, `dim_guest`, `dim_hotel_room`, `dim_booking_details`, `dim_entity`) and a fact table (`fct_bookings`) using dbt.
- Ensure data quality with Soda checks for both raw and transformed data.
- Orchestrate the pipeline using Apache Airflow and Astronomer Astro with Cosmos for dbt integration.

## Architecture

```mermaid
graph LR
    A[Source Data (CSV)<br><img src="https://via.placeholder.com/20?text=📄" alt="File Icon"/>] --> B[Upload to GCP Cloud Storage<br><img src="https://via.placeholder.com/20?text=☁️" alt="Cloud Icon"/>]
    B --> C[Extract & Load to BigQuery<br><img src="https://via.placeholder.com/20?text=📊" alt="Database Icon"/>]
    C --> D[Raw Data Quality Check (Soda)<br><img src="https://via.placeholder.com/20?text=✅" alt="Checkmark Icon"/>]
    D --> E[Transform with dbt<br><img src="https://via.placeholder.com/20?text=⚙️" alt="Gear Icon"/>]
    E --> F[Transformed Data Quality Check (Soda)<br><img src="https://via.placeholder.com/20?text=✅" alt="Checkmark Icon"/>]

    subgraph GCP
        B
        C
    end

    class A,external;
    class F,external;
```
