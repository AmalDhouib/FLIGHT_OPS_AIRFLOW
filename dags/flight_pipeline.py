# Import sys to access system functions and command-line arguments
import sys
from pathlib import Path 
from datetime import datetime,timedelta 
from airflow import DAG 
from airflow.operators.python import PythonOperator
# Python does not automatically search in /opt/airflow/,
# so we add it to sys.path to allow imports from project folders like scripts/ or dags/
AIRFLOW_HOME =Path("/opt/airflow/")

if str(AIRFLOW_HOME) not in sys.path:
   sys.path.insert(0,str(AIRFLOW_HOME))
from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import load_gold_to_snowflake
default_args = {
   "Owner":"airflow",
   "retries":0,
   "retry_delay":timedelta(minutes=1),

} 
with DAG(
   dag_id="flights_ops_medallion_pipe",
   default_args=default_args,
   start_date=datetime(2026,5,8),
   schedule_interval="*/30 * * * *",
   catchup=False,
    
   )as dag:
         bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion
    )
         silver = PythonOperator(task_id="silver_transform",python_callable=run_silver_transform,)
         gold = PythonOperator(task_id = "gold_aggregate",python_callable=run_gold_aggregate)
         load_to_snowflake = PythonOperator(
        task_id = "load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
    )      
         bronze >> silver >> gold >> load_to_snowflake