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
gcloud dataflow jobs run stock-streaming-demo \
--gcs-location=gs://charlie-feng-contino-dataflow/template \
--region=asia-east1 \
--parameters \
"inputTopic=projects/charlie-feng-contino/topics/stock-realtime,\
tableSpec=charlie-feng-contino:samples.alpha_stock,\
windowSize=2"


# New Stock daily archive batch collector
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DaillySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--output=gs://charlie-feng-contino-dataflow/stock-archive \
--runner=DirectRunner"

# New Stock daily archive batch loader 
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DailyLoader4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--input=gs://charlie-feng-contino-dataflow/stock-archive \
--output=gs://charlie-feng-contino-dataflow/stock-archive \
--runner=DirectRunner"


# New Stock intraday batch collector 
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.IntradaySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--outputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--runner=DirectRunner"

# New Stock intraday streaming loader 
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.streaming.IntradayStream4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--projectId=$PROJECT_NAME \
--inputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--bqTableSpec=stock.intraday \
--btInstanceId=stock-intraday \
--btTableId=stock-intraday \
--runner=DirectRunner \
--windowSize=2"