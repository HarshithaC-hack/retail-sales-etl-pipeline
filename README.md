
# Retail ETL Pipeline with Airflow + Docker

## How to Run

1. Make sure Docker Desktop is installed and running.
2. From this project folder, run:
   ```bash
   docker compose up airflow-init
   docker compose up
   ```
3. Open Airflow at [http://localhost:8080](http://localhost:8080)
4. Enable the `retail_sales_etl` DAG and trigger a run.

ETL logic is in `dags/scripts/` and runs through Bronze → Silver → Gold layers.
