# Uber Data Engineering Project

End-to-end streaming data pipeline built on Azure Databricks using Event Hubs, ADLS Gen2, and Azure Data Factory.

## Architecture

```
Azure Event Hubs (live rides)
        │
        ▼
  Bronze Layer  ←──  ADLS Gen2 (bulk/static data via ADF)
  (raw streaming)
        │
        ▼
  Silver Layer  (merged streaming + static, OBT)
        │
        ▼
  Gold Layer    (star schema: Fact + Dimensions)
        │
        ▼
  AI/BI Dashboard (Uber Rides — Business & KPI)
```

## Project Structure

```
├── data/                          # Raw source data (JSON files uploaded to ADLS)
├── notebooks/
│   ├── bronze_adls_ingestion.py   # One-time bulk load from ADLS → Bronze Delta tables
│   ├── silver_OBT.py              # Exploration & Jinja template for OBT SQL generation
│   └── explorations/
│       ├── data_checking.py                         # Ad-hoc data validation queries
│       └── sample_streaming_eventhub_exploration.py # EventHub streaming test
├── pipelines/
│   ├── Bronze/
│   │   └── bronze_streaming_ingestion.py  # SDP: Kafka/EventHub → streaming_raw_rides
│   ├── Silver/
│   │   ├── silver_merge.py                # SDP: bulk load + streaming merge → silver_staging_rides
│   │   └── silver_OBT.sql                 # SDP: streaming OBT with dimension joins → silver_obt
│   └── Gold/
│       └── model.py                       # SDP: Auto CDC star schema → Gold Dim + Fact tables
└── README.md
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion (streaming) | Azure Event Hubs (Kafka protocol) |
| Ingestion (batch) | Azure Data Factory → ADLS Gen2 |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Databricks Lakeflow Spark Declarative Pipelines |
| Catalog | Unity Catalog (`uber.bronze / silver / gold`) |
| Orchestration | Databricks Lakeflow Jobs (trigger: table update on gold tables) |
| BI | Databricks AI/BI Dashboard |

## Unity Catalog Structure

```
uber
├── bronze
│   ├── streaming_raw_rides   (raw Kafka payload)
│   ├── bulk_rides            (historical bulk load)
│   ├── map_cities
│   ├── map_vehicle_types
│   ├── map_vehicle_makes
│   ├── map_payment_methods
│   ├── map_ride_statuses
│   └── map_cancellation_reasons
├── silver
│   ├── silver_staging_rides  (merged stream + bulk)
│   └── silver_obt            (OBT with dimension joins)
└── gold
    ├── fact
    ├── dim_driver
    ├── dim_passenger
    ├── dim_vehicle
    ├── dim_payment
    ├── dim_booking
    └── dim_location          (SCD Type 2)
```

## Secrets

This project uses Databricks Secrets (`uber-secrets` scope):
- `adls-access-token` — SAS token for ADLS Gen2
- `Eventhub-listen-policy` — Event Hubs connection string

## ADF Pipeline

Azure Data Factory is used to copy static/reference data from GitHub → ADLS Gen2 → Bronze Delta tables.
Connect ADF to this repository via **Manage → Git configuration** in ADF Studio for version control.
