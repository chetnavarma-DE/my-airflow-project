from datetime import datetime, timedelta
import os, csv, requests, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL,IBM").split(",")
S3_BUCKET = os.getenv("S3_BUCKET", "airflowsnowde")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

def download_eod_prices(**context):
    ds = context["ds"]
    out_path = f"/tmp/eod_{ds}.csv"
    rows = []
    if ALPHAVANTAGE_API_KEY:
        for sym in SYMBOLS:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": sym,
                "apikey": ALPHAVANTAGE_API_KEY,
                "outputsize": "compact",
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json().get("Time Series (Daily)", {})
            if ds in data:
                drow = data[ds]
                rows.append([sym, ds, drow["1. open"], drow["2. high"],
                             drow["3. low"], drow["4. close"], drow["6. volume"]])
    if not rows:
        sample = "/usr/local/airflow/data_samples/eod_pricing_sample.csv"
        pd.read_csv(sample).to_csv(out_path, index=False)
    else:
        pd.DataFrame(rows, columns=["symbol","trade_date","open","high","low","close","volume"]).to_csv(out_path, index=False)
    return out_path

with DAG(
    dag_id="batch_eod_pricing_dag",
    start_date=datetime(2025,1,1),
    schedule_interval="5 21 * * 1-5",   # weekdays 21:05 UTC
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["securities","batch"],
) as dag:

    download = PythonOperator(
        task_id="download_eod_prices",
        python_callable=download_eod_prices,
    )

    upload = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/tmp/eod_{{ ds }}.csv",
        dest_bucket=S3_BUCKET,
        dest_key="market/bronze/eod/dt={{ ds }}/eod_prices_{{ ds }}.csv",
        aws_conn_id="aws_default",
        replace=True,
    )

    copy = SnowflakeOperator(
        task_id="copy_to_raw",
        snowflake_conn_id="snowflake_default",
        sql="""
        COPY INTO SEC_PRICING.RAW.RAW_EOD_PRICES
        FROM @SEC_PRICING.RAW.EXT_BRONZE/eod/dt={{ ds }}/
        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
        """,
    )

    merge = SnowflakeOperator(
        task_id="merge_core",
        snowflake_conn_id="snowflake_default",
        sql="CALL CORE.SP_MERGE_EOD_PRICES();"
    )

    download >> upload >> copy >> merge
