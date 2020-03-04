export PROJECT_ID=charlie-feng-contino
export GOOGLE_APPLICATION_CREDENTIALS=/Users/charlie/Downloads/credential.json



# New Stock daily archive batch collector
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DaillySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--output=gs://alpha-vantage-landing-zone/stock \
--runner=DirectRunner"

##Staging Job
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DaillySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
--symbol=MSFT \
--output=gs://alpha-vantage-landing-zone/stock \
--stagingLocation=gs://alpha-vantage-dataflow-staging/staging \
--tempLocation=gs://alpha-vantage-dataflow-staging/temp \
--templateLocation=gs://alpha-vantage-dataflow-staging/templates/DaillySubscriber4Stock \
--runner=DataflowRunner"

## Trigger Job
gcloud dataflow jobs run stock-daily-subscriber \
--gcs-location=gs://alpha-vantage-dataflow-staging/templates/DaillySubscriber4Stock \
--region=asia-east1





# New Stock daily archive batch loader 
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DailyLoader4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--input=gs://alpha-vantage-landing-zone/stock \
--output=gs://alpha-vantage-staging-zone/stock \
--runner=DirectRunner"

## Staging Job
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DailyLoader4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
--symbol=MSFT \
--input=gs://alpha-vantage-landing-zone/stock \
--output=gs://alpha-vantage-staging-zone/stock \
--stagingLocation=gs://alpha-vantage-dataflow-staging/staging \
--tempLocation=gs://alpha-vantage-dataflow-staging/temp \
--templateLocation=gs://alpha-vantage-dataflow-staging/templates/DailyLoader4Stock \
--runner=DataflowRunner"

## Trigger Job
gcloud dataflow jobs run stock-daily-loader \
--gcs-location=gs://alpha-vantage-dataflow-staging/templates/DailyLoader4Stock \
--region=asia-east1





# New Stock intraday batch collector 
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.IntradaySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--outputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--runner=DirectRunner"

## Staging Job
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.IntradaySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--outputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--stagingLocation=gs://alpha-vantage-dataflow-staging/staging \
--tempLocation=gs://alpha-vantage-dataflow-staging/temp \
--templateLocation=gs://alpha-vantage-dataflow-staging/templates/IntradaySubscriber4Stock \
--runner=DataflowRunner"

## Trigger Job
gcloud dataflow jobs run stock-intraday-subscriber \
--gcs-location=gs://alpha-vantage-dataflow-staging/templates/IntradaySubscriber4Stock \
--region=asia-east1






# New Stock intraday streaming loader 
## Local DirectRunner
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.streaming.IntradayStream4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--projectId=$PROJECT_ID \
--windowSize=2 \
--inputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--bqTableSpec=stock.intraday \
--btInstanceId=stock-intraday \
--btTableId=stock-intraday \
--runner=DirectRunner"

## Staging Job
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.streaming.IntradayStream4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--projectId=$PROJECT_ID \
--windowSize=2 \
--inputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--bqTableSpec=stock.intraday \
--btInstanceId=stock-intraday \
--btTableId=stock-intraday \
--stagingLocation=gs://alpha-vantage-dataflow-staging/staging \
--tempLocation=gs://alpha-vantage-dataflow-staging/temp \
--templateLocation=gs://alpha-vantage-dataflow-staging/templates/IntradayStream4Stock \
--runner=DataflowRunner"

## Trigger Job
gcloud dataflow jobs run stock-intraday-stream-loader \
--gcs-location=gs://alpha-vantage-dataflow-staging/templates/IntradayStream4Stock \
--region=asia-east1






















































# Local DirectRunner
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
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
--project=$PROJECT_ID \
--inputTopic=projects/charlie-feng-contino/topics/stock-realtime \
--tableSpec=samples.alpha_stock \
--windowSize=2 \
--stagingLocation=gs://charlie-feng-contino-dataflow/staging \
--tempLocation=gs://charlie-feng-contino-dataflow/temp \
--templateLocation=gs://charlie-feng-contino-dataflow/template \
--runner=DataflowRunner"

## Trigger job
gcloud dataflow jobs run stock-streaming-demo \
--gcs-location=gs://charlie-feng-contino-dataflow/template \
--region=asia-east1 \
--parameters \
"inputTopic=projects/charlie-feng-contino/topics/stock-realtime,\
tableSpec=charlie-feng-contino:samples.alpha_stock,\
windowSize=2"