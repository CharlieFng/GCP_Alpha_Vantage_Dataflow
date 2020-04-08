import json
import os
import random
import string
import googleapiclient.discovery

'''
gcloud functions deploy stockLoaderTrigger \
    --trigger-resource gs://alpha-vantage-landing-zone \
    --trigger-event google.storage.object.finalize \
    --set-env-vars PROJECT_ID=charlie-feng-contino,REGION=asia-east1,TEMPLATE_NAME=DailyLoader4Stock,STAGING_BUCKET=alpha-vantage-dataflow-staging \
    --runtime python37 \
    --allow-unauthenticated
'''

function_name = os.getenv('FUNCTION_NAME')

project_id = os.getenv('PROJECT_ID')
region = os.getenv('REGION')

staging_bucket = os.getenv('STAGING_BUCKET')
template_name = os.getenv('TEMPLATE_NAME')

temp_location = 'gs://{bucket}/temp'.format(bucket=staging_bucket)
template_location = 'gs://{bucket}/templates/{name}'.format(bucket=staging_bucket,name=template_name)
output_bucket = 'alpha-vantage-staging-zone'

dataflow = googleapiclient.discovery.build('dataflow', 'v1b3')

def stockLoaderTrigger(event, context):
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('Path: {}'.format(event['name']))

    fileName = str(event['name']).split("/")[-1]
    print('Generated file is {}'.format(fileName))

    if(fileName.endswith('avro')):
        # Generate a job name based on the Cloud Function name and a random ID.
        job_id = ''.join(random.choice(string.ascii_lowercase +
                                        string.digits) for _ in range(8))
        symbol = fileName.split('-')[0]
        print('Stock symbol is: {}'.format(symbol))

        job_name = 'stock_daily_loader_{}_{}'.format(symbol.lower(),job_id)
        print('Dataflow job name is: {}'.format(job_name))

        output_path = '/'.join((str(event['name'])).split("/")[:-1])
        print('Output path is: {}'.format(output_path))

        # Configure input and output locations.
        input_file = 'gs://{}/{}'.format(event['bucket'], event['name'])
        print("Input file is: {}".format(input_file))

        output_location = 'gs://{}/{}/{}'.format(output_bucket, output_path, symbol)
        print("Output location is {}".format(output_location))

        print("Launching Dataflow job for template '{}'".format(temp_location))
        result = dataflow.projects().locations().templates().launch(
            projectId = project_id,
            location = region,
            body = {
                    "jobName": job_name,
                    "parameters": {
                        "input": input_file,
                        "output": output_location
                    },
                    "environment": {
                        "workerRegion": region,
                        "tempLocation": temp_location
                    } 
            },
            gcsPath = template_location
        ).execute()
        job = result['job']
        print("Dataflow job '{}' created".format(job['name']))
        return job['name']
    
    return


