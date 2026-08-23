from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import subprocess
import pendulum


# Base paths
BASE_PATH = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/indian_stock_analytics"
PYTHON_PATH = "/opt/homebrew/bin/python3.10"


def scrap_financial_data():
    """Scrape financial data from screener.in"""
    result = subprocess.run(
        [
            PYTHON_PATH,
            f"{BASE_PATH}/scrap_financial_local.py"
        ],
        capture_output=True,
        text=True,
        cwd=BASE_PATH  # Set working directory
    )
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print("Return code:", result.returncode)
    
    if result.returncode != 0:
        raise RuntimeError(f"Scraping failed with return code {result.returncode}. Error: {result.stderr}")
    
    return result.stdout


def load_financial_data():
    """Load scraped financial data into PostgreSQL"""
    result = subprocess.run(
        [
            PYTHON_PATH,
            f"{BASE_PATH}/load_financial_data.py"
        ],
        capture_output=True,
        text=True,
        cwd=BASE_PATH  # Set working directory
    )
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print("Return code:", result.returncode)
    
    if result.returncode != 0:
        raise RuntimeError(f"Loading failed with return code {result.returncode}. Error: {result.stderr}")
    
    return result.stdout


default_args = {
    'owner': 'kunal.nandwana',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 17, tzinfo=pendulum.timezone("Asia/Kolkata")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


nse_dataset = Dataset("scrap_load_financial_load")

dag = DAG(
    'weekly_screener_extraction_load',
    default_args=default_args,
    description='A DAG to scrape and load financial data from screener.in',
    schedule=None,
    catchup=False,
    tags=['screener', 'financial', 'weekly'],
)

scrap_financial_data_task = PythonOperator(
    task_id='scrap_financial_data',
    python_callable=scrap_financial_data,
    dag=dag,
)

load_financial_data_task = PythonOperator(
    task_id='load_financial_data',
    python_callable=load_financial_data,
    dag=dag,
)

update_dataset = DummyOperator(
    task_id='update_financial_loaded_dataset',
    outlets=[nse_dataset],
    dag=dag,
)

# Task dependencies
scrap_financial_data_task >> load_financial_data_task >> update_dataset
