export GOOGLE_CLOUD_PROJECT=charlie-feng-contino
export GOOGLE_APPLICATION_CREDENTIALS=/Users/charlie/Downloads/credential.json

mvn package appengine:run

mvn package appengine:deploy