# Exposure Notification Private Analytics Ingestion

This repository contains batch processing jobs that can be used to ingest
private data shares according to the Exposure Notification Private Analytics
protocol.

## Testing

Please be sure to set SERVICE_ACCOUNT_KEY_PATH in IngestionPipelineTest.java before running. To run all unit tests:

```shell script
mvn test
```

## Running

### Locally

#### Be sure to replace serviceAccountKey default path before running.
```shell script
mvn -Pdirect-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--output=counts --serviceAccountKey='PATH/TO/SERVICE_ACCOUNT_KEY.json' --firebaseProjectId='appa-firebase-test'"
```

### On Cloud

```shell script
mvn -Pdataflow-runner compile exec:java  -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline  -Dexec.args="--project=appa-ingestion --stagingLocation=gs://appa-batch-output/staging/  --output=gs://appa-batch-output/output --runner=DataflowRunner  --region=us-central1"
```

## Deploying

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

```shell script
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--project=appa-ingestion --runner=DataflowRunner --region=us-central1 --stagingLocation=gs://appa-batch-output/staging/ --templateLocation=gs://appa-test-bucket/templates/test-template-1"
```
