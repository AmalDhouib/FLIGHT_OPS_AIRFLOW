# Flights Operations Medallion Pipeline

## Project Overview

This project implements an end-to-end data pipeline using the Medallion Architecture (**Bronze → Silver → Gold**) to process flight operations data and load business KPIs into Snowflake for analytics and monitoring.

The pipeline is orchestrated using Apache Airflow and follows a modern data engineering approach with data ingestion, transformation, aggregation, and warehouse loading.

The final objective is to automate data collection and integration into the database, ensuring reliable and structured data availability for dashboard visualization and operational analysis.

# Architecture

## Medallion Layers

### Bronze Layer — Raw Data Ingestion

The Bronze layer stores raw flight data extracted from the source API without major transformations.

### Silver Layer — Cleaned and Standardized Data

The Silver layer transforms the raw OpenSky data into a structured and simplified dataset. It assigns meaningful column names, selects the relevant fields, and stores the result as a CSV file that can be reused by the next pipeline steps.

### Gold Layer — Business Aggregation

The Gold layer generates results such as:
- total flights per country
- average flight velocity
- number of flights on ground

=
---

# Technologies Used

## Data Orchestration

- Apache Airflow

## Data Processing

- Python
- Pandas

## Storage

- Local CSV files (Bronze / Silver / Gold)
- Snowflake Data Warehouse

## Infrastructure

- Docker
- Docker Compose



---

# Project Structure

```text
project/
│
├── dags/
│   └── flight_pipeline.py
│
├── scripts/
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_aggregate.py
│   └── load_gold_to_snowflake.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker-compose.yml
│
└── README.md
```

---

# DAG Workflow

## DAG Name

`flights_ops_medallion_pipe`

## Tasks

### 1. bronze_ingest

Extract raw flight data from the source API and store it in Bronze layer.

### 2. silver_transform

Clean and standardize the raw data before saving into Silver layer.

### 3. gold_aggregate

Generate KPI aggregations and save the results into Gold layer.

### 4. load_gold_to_snowflake

Load Gold KPIs into Snowflake using SQL MERGE logic to support UPSERT operations.

---

# Snowflake Target Table

## Table Name

`FLIGHTS.PUBLIC.FLIGHT_KPIS`

## Table Structure

```sql
CREATE OR REPLACE TABLE FLIGHT_KPIS (
    WINDOW_START TIMESTAMP,
    ORIGIN_COUNTRY STRING,
    TOTAL_FLIGHT NUMBER,
    AVG_VELOCITY FLOAT,
    ON_GROUND NUMBER,
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```


# Airflow Connection

## Snowflake Connection ID

```text
flight_snowflake
```

This Airflow connection stores:

- username
- password
- account
- warehouse
- database
- schema
- role

Using `BaseHook` ensures secure credential management.

---

# Data Source

## OpenSky Network API

This project uses flight data from the official OpenSky Network API:

https://openskynetwork.github.io/opensky-api/rest.html

The OpenSky Network API provides aircraft state vector data. In this project, the data is used as the raw source for the flight operations pipeline.

The collected data can include information such as:

- aircraft identifier (`icao24`)
- callsign
- origin country
- longitude and latitude
- altitude
- velocity
- heading
- vertical rate
- on-ground status
- timestamps related to aircraft position updates

In this project, these data are processed through Bronze, Silver, and Gold layers to generate aggregated results by origin country.

The final Snowflake table can then be used to create simple dashboards directly in Snowflake, for example to visualize:

- total flights by country
- average velocity by country
- number of aircraft on ground by country

This dashboard part is a possible usage of the Snowflake table, but the main implemented scope of the project is the data pipeline itself.

---

# Installation

## Install Project Dependencies

```bash
pip install -r requirements.txt
```

This command installs all required Python libraries used by the project.

## Start the Environment

```bash
docker compose up -d
```

This command starts all services in detached mode, including Airflow and the required project environment.

---

# Running the Project

## Start Airflow

```bash
docker compose up -d
```



## Trigger DAG

From the Airflow UI:

- enable the DAG
- trigger manual run
- monitor task execution

