from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,         # change to "@daily" later
    catchup=False,
    tags=["demo"]
)
def hello():
    @task
    def say_hi():
        print("Hello from Astronomer Cloud!")
    say_hi()

dag = hello()
#first dag
