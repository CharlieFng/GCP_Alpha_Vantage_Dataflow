import airflow
import pytz
import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from dependencies import config
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator

'''
gsutil cp stock_intraday_trigger_dag.py gs://asia-northeast1-alpha-vanta-10c9b740-bucket/dags/
gsutil -m cp -R dependencies/ gs://asia-northeast1-alpha-vanta-10c9b740-bucket/dags/
'''


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020,3,16).astimezone("US/Eastern"),
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

with DAG('stock_intraday_trigger_dag', default_args=default_args,schedule_interval='*/30 9-17 * * 1-5',catchup = False) as dag:

    for symbol in stock_symbols:
        job_trigger_tasks.append(
            DataFlowJavaOperator(
                task_id = 'stock_intraday_subscriber_{}'.format(symbol.lower()),
                jar = config.uber_jar_location,
                job_class = config.stock_intraday_subscriber_class_name,
                options = {
                    'symbol': symbol,
                    'interval': '1min',
                    'outputTopic': config.intraday_topic
                }
            )
        )


    dummy = DummyOperator(task_id='wait_all_jobs_triggered')

     
    job_trigger_tasks >> dummy