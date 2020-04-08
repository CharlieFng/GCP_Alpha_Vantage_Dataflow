from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

class JobTriggerUtil:
    @staticmethod
    def trigger_job(projectId, gcsPath, body):
        print(" --------in triggerjobs---------")
        credentials = GoogleCredentials.get_application_default()
        service = build('dataflow', 'v1b3', credentials=credentials)
        request = service.projects().templates().launch(projectId=projectId, gcsPath=gcsPath, body=body)
        response = request.execute()
        print("-----execute function have been called-------")
        print(response)
        print("Dataflow job '{}' created".format(response['job']['name']))