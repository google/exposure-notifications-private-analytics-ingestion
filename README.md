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

### Locally

#### Be sure to set serviceAccountKey before running.
```shell script
mvn -Pdirect-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--output=counts --serviceAccountKey='PATH/TO/SERVICE_ACCOUNT_KEY.json' --firebaseProjectId='appa-firebase-test'"
```

### On Cloud

#### Be sure to set serviceAccountKey before running.
```shell script
mvn -Pdataflow-runner compile exec:java  -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline  -Dexec.args="--project=appa-ingestion --stagingLocation=gs://appa-batch-output/staging/  --output=gs://appa-batch-output/output --runner=DataflowRunner  --region=us-central1 --serviceAccountKey='PATH/TO/SERVICE_ACCOUNT_KEY.json' --firebaseProjectId='appa-firebase-test'"
```

## Deploying

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

```shell script
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--project=appa-ingestion --runner=DataflowRunner --region=us-central1 --stagingLocation=gs://appa-batch-output/staging/ --templateLocation=gs://appa-test-bucket/templates/test-template-1"
```
