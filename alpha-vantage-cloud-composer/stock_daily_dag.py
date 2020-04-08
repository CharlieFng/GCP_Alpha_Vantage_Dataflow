import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from dependencies import config
from dependencies.trigger_job_util import JobTriggerUtil

'''
gsutil cp stock_daily_dag.py gs://australia-southeast1-alpha--dfbc12f3-bucket/dags/
gsutil -m cp -R dependencies/ gs://australia-southeast1-alpha--dfbc12f3-bucket/dags/
'''

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'execution_timeout': timedelta(hours=5)
}


def stock_daily_subscriber():
    try:
        body = {
            "jobName": "{jobname}".format(jobname=config.job_name),
            "parameters": config.parameters,
            "environment": {
                "tempLocation": config.temp_location,
                "workerRegion": config.worker_region
            }
        }
        print("Job body: {}".format(body))
        JobTriggerUtil.trigger_job(projectId=config.project_name,gcsPath=config.template_gcs_path,body=body)
    except Exception as ex:
        print(['Exception', ex])
  

with DAG('stock_daily_dag', default_args=default_args,schedule_interval='@daily',catchup = False) as dag:
    
    t1 = PythonOperator(task_id='trigger_daily_subscriber',
                            python_callable=stock_daily_subscriber,
                            dag=dag)
    
t1