from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from include.stock_market.tasks import (
    BUCKET_NAME,
    _get_stock_prices,
    _store_prices,
    _get_formated_csv,
)

SYMBOL = "AAPL"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
    on_success_callback=SlackNotifier(
        slack_conn_id="slack",
        channel="stock-market",
        text="The stock_market DAG success"
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id="slack",
        channel="stock-market",
        text="The stock_market DAG FAILED"
    )
)
def stock_market():
    # try to not mix decorator API with the explicit operator API
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None

        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={
            "url": '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
            "symbol": SYMBOL
        }
    )
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={
            "stock": '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}',
        }
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image="airflow/spark-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ task_instance.xcom_pull(task_ids="store_prices") }}',
        }
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formated_csv,
        op_kwargs={
            "path": '{{ task_instance.xcom_pull(task_ids="store_prices") }}',
        }
    )

    formatted_csv_xcom = "{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}"

    load_to_dw = aql.load_file(
        task_id="load_to_dw",
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id="minio",
        ),
        output_table=Table(
            name="stock_market",
            conn_id="postgres",
            metadata=Metadata(
                schema="public"
            )
        )
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw




stock_market()


