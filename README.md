# Medallion Architecture Pipeline

A simple data pipeline: **API → Bronze (S3) → Silver (S3) → Gold (S3) → Snowflake**.

## Setup

1. Create a `.env` file with AWS and Snowflake credentials.
2. Install dependencies: `pip install -r requirements.txt`
3. Use a virtual environment (e.g. `.venv`).

## Run (in order)

1. **Ingest to Bronze:** `python src/ingest_bronze.py`
2. **Bronze → Silver:** `python src/bronze_to_silver.py`
3. **Silver → Gold:** `python src/silver_to_gold.py`
4. **Load to Snowflake:** `python src/load_gold_to_snowflake.py`

## Data source

[JSONPlaceholder](https://jsonplaceholder.typicode.com/posts) (posts).
