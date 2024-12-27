from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
from random import randint


def _generate_random_number():
    number = randint(0, 1000)
    print(f"generate : {number}")
    return number

def _odd_or_even(number):
    result = "odd"
    if int(number)%2 == 0:
        result = "even"
    print(f"{number} is {result}")
    return result

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"]
)
def odd_even_machine():
    generate_random_number = PythonOperator(
        task_id="generate_random_number",
        python_callable=_generate_random_number,
        # op_kwargs={
        #     "url": '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
        #     "symbol": SYMBOL
        # }
    )

    odd_or_even = PythonOperator(
        task_id="odd_or_even",
        python_callable=_odd_or_even,
        op_kwargs={
            "number": '{{ task_instance.xcom_pull(task_ids="generate_random_number") }}',
        }
    )
    
    generate_random_number >> odd_or_even




odd_even_machine()



