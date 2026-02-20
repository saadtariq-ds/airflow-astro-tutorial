from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


## Define Task 1
def process_data():
    print("Processing data...")

## Define Task 2
def train_model():
    print("Training model...")

## Define Task 3
def evaluate_model():
    print("Evaluating model...")


## Define the DAG
with DAG(
    "ml_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
) as dag:
    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=process_data,
    )
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )
    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )
    preprocess_task >> train_task >> evaluate_task