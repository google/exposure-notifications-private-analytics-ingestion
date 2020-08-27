# Exposure Notification Private Analytics Ingestion

This repository contains batch processing jobs that can be used to ingest
private data shares according to the Exposure Notification Private Analytics
protocol.

## Building/Running Tests

To run all unit tests:

```shell script
mvn test
```

## Running

### Locally

```shell script
mvn -Pdirect-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--output=counts"
```

### On Cloud

```shell script
mvn -Pdataflow-runner compile exec:java  -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline  -Dexec.args="--project=appa-ingestion --stagingLocation=gs://appa-batch-output/staging/  --output=gs://appa-batch-output/output --runner=DataflowRunner  --region=us-central1"
```
