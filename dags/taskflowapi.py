from airflow import DAG
from airflow.decorators import task
from datetime import datetime

## Define the DAG
with DAG(
    dag_id="maths_operations_with_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False
) as dag:
    # Task 1
    @task
    def start_number():
        initial_number = 10
        print(f"Initial number: {initial_number}")
        return initial_number
    
    # Task 2
    @task
    def add_five(number):
        result = number + 5
        print(f"After adding 5 to {number}: {result}")
        return result
    
    # Task 3
    @task
    def multiply_by_two(number):
        result = number * 2
        print(f"After multiplying {number} by 2: {result}")
        return result
    
    # Task 4
    @task
    def subtract_four(number):
        result = number - 4
        print(f"After subtracting 4 from {number}: {result}")
        return result
    
    # Task 5
    @task
    def square_number(number):
        result = number ** 2
        print(f"After squaring {number}: {result}")
        return result
    
    ## Set up the task dependencies
    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_by_two(added_value)
    subtracted_value = subtract_four(multiplied_value)
    final_value = square_number(subtracted_value)