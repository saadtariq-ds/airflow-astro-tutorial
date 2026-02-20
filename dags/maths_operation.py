"""
We'll define a DAG where the tasks are as follows:

Task 1: Start with the Initial Number (let's say 10)
Task 2: Add 5 to the Initial Number
Task 3: Multiply the result by 2
Task 4: Subtract 4 from the result
Task 5: Compute the square of the result
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define Task 1
def start_number(**context):
    context["ti"].xcom_push(key="current_value", value=10)
    print("Starting with the initial number: 10")

## Define Task 2
def add_five(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="start_number")
    new_value = current_value + 5
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"Adding 5 to the initial number: {current_value} + 5 = {new_value}")

## Define Task 3
def multiply_by_two(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="add_five")
    new_value = current_value * 2
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"Multiplying the result by 2: {current_value} * 2 = {new_value}")

## Define Task 4
def subtract_four(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="multiply_by_two")
    new_value = current_value - 4
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"Subtracting 4 from the result: {current_value} - 4 = {new_value}")

## Define Task 5
def compute_square(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="subtract_four")
    new_value = current_value ** 2
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"Computing the square of the result: {current_value} ** 2 = {new_value}")


## Define the DAG
with DAG(
    dag_id="maths_operation",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
) as dag:
    start_task = PythonOperator(
        task_id="start_number",
        python_callable=start_number,
    )
    add_task = PythonOperator(
        task_id="add_five",
        python_callable=add_five,
    )
    multiply_task = PythonOperator(
        task_id="multiply_by_two",
        python_callable=multiply_by_two,
    )
    subtract_task = PythonOperator(
        task_id="subtract_four",
        python_callable=subtract_four,
    )
    square_task = PythonOperator(
        task_id="compute_square",
        python_callable=compute_square,
    )
 
    start_task >> add_task >> multiply_task >> subtract_task >> square_task