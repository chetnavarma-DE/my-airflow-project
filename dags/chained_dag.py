from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id="chained_dag", start_date=datetime(2024,1,1), schedule=None, catchup=False)
def pipeline():
    @task
    def extract():
        return "data"

    @task
    def transform(data: str):
        return data.upper()

    @task
    def load(result: str):
        print(f"Loaded: {result}")

    load(transform(extract()))

dag = pipeline()
