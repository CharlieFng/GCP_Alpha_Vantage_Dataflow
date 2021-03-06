export PROJECT_ID=charlie-feng-contino
export GOOGLE_APPLICATION_CREDENTIALS=/Users/charlie/Downloads/credential.json


# New Stock daily archive batch collector
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DaillySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=AMZN \
--output=gs://alpha-vantage-landing-zone/stock \
--runner=DirectRunner"

## Dataflow Runner
java -cp target/alpha-vantage-dataflow-bundled-1.0.jar \
club.charliefeng.dataflow.batch.DaillySubscriber4Stock \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=asia-east1 \
  --tempLocation=gs://alpha-vantage-dataflow-staging/temp \
  --stagingLocation=gs://alpha-vantage-dataflow-staging/staging/uber \
  --symbol=MSFT \
  --output=gs://alpha-vantage-landing-zone/stock 
  




# New Stock daily archive batch loader 
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DailyLoader4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--input=gs://alpha-vantage-landing-zone/stock/2020-04-08/MSFT-*.avro \
--output=gs://alpha-vantage-staging-zone/stock/2020-04-08/MSFT \
--runner=DirectRunner"

## Staging Job
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.DailyLoader4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
--region=asia-east1 \
--stagingLocation=gs://alpha-vantage-dataflow-staging/staging \
--tempLocation=gs://alpha-vantage-dataflow-staging/temp \
--templateLocation=gs://alpha-vantage-dataflow-staging/templates/DailyLoader4Stock \
--runner=DataflowRunner"

## Trigger Job
gcloud dataflow jobs run stock-daily-loader \
--region=asia-east1 \
--gcs-location=gs://alpha-vantage-dataflow-staging/templates/DailyLoader4Stock \
--parameters=input=gs://alpha-vantage-landing-zone/stock/2020-04-08/AMZN-*.avro,\
output=gs://alpha-vantage-staging-zone/stock/2020-04-08/AMZN




# New Stock intraday batch collector 
## Local DirectRunner
mvn compile exec:java -Dexec.mainClass=club.charliefeng.dataflow.batch.IntradaySubscriber4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--symbol=MSFT \
--interval=1min \
--outputTopic=projects/charlie-feng-contino/topics/stock-intraday \
--runner=DirectRunner"

## Dataflow Runner
java -cp target/alpha-vantage-dataflow-bundled-1.0.jar \
club.charliefeng.dataflow.batch.IntradaySubscriber4Stock \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=asia-east1 \
  --tempLocation=gs://alpha-vantage-dataflow-staging/temp \
  --stagingLocation=gs://alpha-vantage-dataflow-staging/staging/uber \
  --symbol=MSFT \
  --interval=1min \
  --outputTopic=projects/charlie-feng-contino/topics/stock-intraday 






# New Stock intraday streaming loader 
## Local DirectRunner
mvn compile exec:java \
-Dexec.mainClass=club.charliefeng.dataflow.streaming.IntradayStream4Stock \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--projectId=$PROJECT_ID \
--stocks=MSFT,AAPL,AMZN,GOOGL \
--windowSize=5 \
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
--stocks=MSFT,AAPL,AMZN,GOOGL \
--windowSize=5 \
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



