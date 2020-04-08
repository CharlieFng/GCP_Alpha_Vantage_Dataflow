import airflow
import pytz
import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from dependencies import config
from airflow.operators.dummy_operator    import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator

'''
gsutil cp stock_daily_trigger_dag.py gs://asia-northeast1-alpha-vanta-62e8283b-bucket/dags/
gsutil -m cp -R dependencies/ gs://asia-northeast1-alpha-vanta-62e8283b-bucket/dags/
'''


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020,4,6).astimezone("US/Eastern"),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'execution_timeout': timedelta(hours=5),
    'dataflow_default_options': {
        'project': config.project_name,
        'region': config.worker_region,
        'stagingLocation': config.uber_staging_location,
        'gcpTempLocation': config.temp_location
    }
}

stock_symbols = ['AAPL','AMZN','GOOGL','MSFT']
job_trigger_tasks = []  
post_check_tasks = []

current_date = datetime.now(pytz.timezone("US/Eastern")).date()
print('Current date in timezone US/Eastern is {}'.format(current_date))

with DAG('stock_daily_trigger_dag', default_args=default_args,schedule_interval='0 9 * * 2-6',catchup = False) as dag:

    for symbol in stock_symbols:
        job_trigger_tasks.append(
            DataFlowJavaOperator(
                task_id = 'stock_daily_subscriber_{}'.format(symbol.lower()),
                jar = config.uber_jar_location,
                job_class = config.stock_daily_subscriber_class_name,
                options = {
                    'symbol': symbol,
                    'output': config.landing_location
                }
            )
        )

        post_check_tasks.append(
            GoogleCloudStorageObjectSensor(
                task_id = 'stock_staging_check_{}'.format(symbol.lower()),
                bucket = config.staging_bucket,
                object = 'stock/{date}/{symbol}/output-00000-of-00001.parquet'.format(date=current_date,symbol=symbol),
                poke_interval = 180,
                timeout = 1200
            )
        )

    dummy = DummyOperator(task_id='wait_all_jobs_triggered')

     
    job_trigger_tasks >> dummy >> post_check_tasks