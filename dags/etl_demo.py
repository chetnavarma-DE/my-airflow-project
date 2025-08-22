from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="etl_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # set to "@daily" later if you want
    catchup=False,
    tags=["demo"]
)
def etl():
    @task
    def extract():
        return {"value": 100}

    @task
    def transform(record: dict):
        record["value"] *= 2
        return record

    @task
    def load(record: dict):
        print(f"Loaded record: {record}")

    load(transform(extract()))

dag = etl()
