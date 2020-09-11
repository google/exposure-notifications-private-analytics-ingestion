# Exposure Notification Private Analytics Ingestion

This repository contains batch processing jobs that can be used to ingest
private data shares according to the Exposure Notification Private Analytics
protocol.

## Testing

### Unit Tests

To run unit tests:

```shell script
mvn test
```

### Integration Tests

Install the [Firebase CLI](https://firebase.google.com/docs/cli), login and
setup the emulator as follows:

```shell script
firebase login
firebase setup:emulators:firestore
```

To run integration tests (includes standing up a Firestore emulator):

```shell script
mvn verify
```

## Running

Set the following local variables that will be used in the commands below.

```shell script
OUTPUT_LOCATION=gs://some/output/folder
FIREBASE_PROJECT_ID=firebase-project-id
GCP_PROJECT_ID=other-project-id
SERVICE_ACCOUNT_KEY=/some/key.json
METRIC=metricOfInterest
```

### Locally

```shell script
mvn -Pdirect-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--output=$OUTPUT_LOCATION --firebaseProjectId=$FIREBASE_PROJECT_ID --serviceAccountKey=$SERVICE_ACCOUNT_KEY --metric=$METRIC"
```

### On Cloud

```shell script
mvn -Pdataflow-runner compile exec:java  -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline  -Dexec.args="--project=$GCP_PROJECT_ID --stagingLocation=$OUTPUT_LOCATION/staging/  --output=$OUTPUT_LOCATION --runner=DataflowRunner  --region=us-central1 --serviceAccountKey=$SERVICE_ACCOUNT_KEY --firebaseProjectId=$FIREBASE_PROJECT_ID"
```

## Deploying

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

```shell script
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--project=appa-ingestion --runner=DataflowRunner --region=us-central1 --stagingLocation=gs://appa-batch-output/staging/ --templateLocation=gs://appa-test-bucket/templates/test-template-1"
```
