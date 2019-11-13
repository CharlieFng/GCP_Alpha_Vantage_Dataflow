export PROJECT_NAME=charlie-feng-contino
export GOOGLE_APPLICATION_CREDENTIALS=/Users/charlie/Downloads/credential.json

# Local DirectRunner
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_NAME \
--inputTopic=projects/charlie-feng-contino/topics/stock-realtime \
--tableSpec=samples.alpha_stock \
--runner=DirectRunner \
--windowSize=2"


# Dataflow Runner

## Staging job
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_NAME \
--inputTopic=projects/charlie-feng-contino/topics/stock-realtime \
--tableSpec=samples.alpha_stock \
--windowSize=2 \
--stagingLocation=gs://charlie-feng-contino-dataflow/staging \
--tempLocation=gs://charlie-feng-contino-dataflow/temp \
--templateLocation=gs://charlie-feng-contino-dataflow/template \
--runner=DataflowRunner"

## Trigger job
gcloud dataflow jobs run stock-streaming \
--gcs-location=gs://charlie-feng-contino-dataflow/template \
--zone=australia-southeast1-a \
--parameters \
"inputTopic=projects/charlie-feng-contino/topics/stock-realtime,\
tableSpec=charlie-feng-contino:samples.alpha_stock,\
windowSize=2"


