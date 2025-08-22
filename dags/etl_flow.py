@dag(dag_id="etl_demo1", start_date=datetime(2024,1,1), schedule="@daily", catchup=False)
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
#update dag
