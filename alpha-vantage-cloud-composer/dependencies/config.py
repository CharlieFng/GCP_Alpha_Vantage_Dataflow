#Project Configurations
project_name='charlie-feng-contino'
worker_region = 'asia-east1'
#template_bucket
template_bucket = 'alpha-vantage-dataflow-staging'
#alpha vantage landing bucket
landing_bucket = 'alpha-vantage-landing-zone'
#alpha vantage staging bucket
staging_bucket = 'alpha-vantage-staging-zone'
#template_name
template_name = 'DaillySubscriber4Stock'
#uber_jar_name
uber_jar_name = 'alpha-vantage-dataflow-bundled-1.0.jar'
#intraday_topic_name
intraday_topic_name = 'stock-intraday'
#Daily Trigger Dataflow Template Constants
job_name ='stock-daily-subscriber'
# ValueProviders like source table name and sink table name
parameters ={"symbol":"MSFT"}
#Template's GCS path
template_gcs_path='gs://{bucket}/templates/{template}'.format(bucket=template_bucket,template=template_name)
#Temprory path location
temp_location='gs://{bucket}/temp'.format(bucket=template_bucket)

uber_staging_location='gs://{bucket}/staging/uber'.format(bucket=template_bucket)

uber_jar_location='gs://{bucket}/staging/uber/{jar}'.format(bucket=template_bucket,jar=uber_jar_name)

stock_daily_subscriber_class_name='club.charliefeng.dataflow.batch.DaillySubscriber4Stock'

stock_intraday_subscriber_class_name='club.charliefeng.dataflow.batch.IntradaySubscriber4Stock'

landing_location='gs://{bucket}/stock'.format(bucket=landing_bucket)

intraday_topic = 'projects/{project_name}/topics/{topic_name}'.format(project_name=project_name,topic_name=intraday_topic_name)