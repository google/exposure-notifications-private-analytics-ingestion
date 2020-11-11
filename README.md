![Integration Test on Push](https://github.com/google/exposure-notifications-private-analytics-ingestion/workflows/Integration%20Test%20on%20Push/badge.svg?branch=main)

# Exposure Notification Private Analytics Ingestion

This repository contains batch processing jobs that can be used to ingest
private data shares according to the Exposure Notification Private Analytics
protocol. It assumes private data shares are uploaded as Firestore documents,
as is done with the Exposure Notification Express apps, and runs an Apache
Beam pipeline to periodically convert them into the format that the private
aggregation algorithms expect, defined in Avro schema format
[here](https://github.com/abetterinternet/prio-server/tree/master/avro-schema).

This repository also contains Firebase configuration to lockdown Firestore
with security rules, and Firebase Remote Config that can be used to
dynamically change the data share creation behavior in an Exposure
Notification Express app.

This implementation happens to make use of Firestore as a convenient way to
send up the packets to a scalable NoSQL db for subsequent batching and aggregation.
Alternative implementations might operate a custom backend endpoint to accumulate
the packets, or use a pubsub mechanism. Since the packets are encrypted on device,
the channel over which the packets travel need not be trusted.

## Before you begin

This repository uses git submodules. You will need to initialize the git submodules 
before being able to compile.
```sh
git submodule update --init
```

Follow the
[Getting started with Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/dataflow/README.md)
page, and make sure you have a Google Cloud project with billing enabled
and a *service account JSON key* set up in your `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
Additionally, you also need the following:

1. Set up a
    [Google Cloud project](https://console.cloud.google.com/projectcreate) or use an existing one.
    Then [import the Google Cloud project into Firebase](https://cloud.google.com/firestore/docs/client/get-firebase).

    ```sh
    export GCP_PROJECT_ID="my-google-cloud-ingestion-project-id"
    export FIREBASE_PROJECT_ID="$GCP_PROJECT_ID"
    ```

1. [Enable APIs](https://console.cloud.google.com/flows/enableapi?apiid=containerregistry.googleapis.com,cloudbuild.googleapis.com):
    Container Registry, Cloud Build

1. [Create an asymmetric key ring](https://cloud.google.com/kms/docs/creating-asymmetric-keys)

<!-- TODO: set the roles needed for the service account -->

## Testing

### Unit Tests

To run unit tests:

```shell script
./mvnw test
```

### Integration Tests

Integration tests go against an actual test project and so need an environment
variable:

```shell script
export PROJECT="my-gcp-project-id"

./mvnw verify
```

## Deploying / Building DataFlow template

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

Setting the following environment variables is useful for the commands below.

```sh
export PROJECT="my-google-cloud-ingestion-project-id"
export PHA_OUTPUT="gs://my-cloud-storage-bucket/output/folder/pha"
export FACILITATOR_OUTPUT="gs://my-cloud-storage-bucket/output/folder/faciliator"
export KEY_RESOURCE_NAME="projects/some-ingestion-project/locations/global/keyRings/some-signature-key-ring/cryptoKeys/some-signature-key/cryptoKeyVersions/1"
```

```sh
export TEMPLATE_LOCATION="gs://my-google-cloud-bucket/templates/local-build-`date +'%Y-%m-%d-%H-%M'`"
export STAGING_LOCATION="gs://my-cloud-storage-bucket/staging"

export BEAM_ARGS=(
    "--runner=DataflowRunner"
    "--project=$GCP_PROJECT_ID"
    "--stagingLocation=$STAGING_LOCATION"
    "--region=us-central1"
    "--templateLocation=$TEMPLATE_LOCATION"
)
./mvnw -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

## Running the pipeline

### Locally

```sh
export BEAM_ARGS=(
    "--project=$PROJECT"
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--PHAOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
)
./mvnw -Pdirect-runner compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

### On Cloud

#### From local build

```sh
export SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

export BEAM_ARGS=(
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--PHAOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
    "--runner=DataflowRunner"
    "--project=$GCP_PROJECT_ID"
    "--region=us-central1"
    "--serviceAccount=$SERVICE_ACCOUNT_EMAIL"
)
./mvnw -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

## Running the Document Deletion Pipeline

### Locally
Set --startTime and --duration to delete documents uploaded between
startTime and startTime + duration. You can use --graceHoursBackward and
--graceHoursForward to further expand this window, as necessary.

```sh
export BEAM_ARGS=(
    "--project=$PROJECT"
)
./mvnw -Pdirect-runner compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.DeletionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

## Running the Document Deletion Pipeline

### Locally
Set --startTime and --duration to delete documents uploaded between
startTime and startTime + duration. You can use --graceHoursBackward and
--graceHoursForward to further expand this window, as necessary.

```sh
export BEAM_ARGS=(
    "--firebaseProjectId=$FIREBASE_PROJECT_ID"
)
./mvnw -Pdirect-runner compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.DeletionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

## Using templates

### Creating a Flex Template

Compile and package into an Uber JAR file.

```sh
./mvnw clean package
```

Build the Flex Template.

```sh
export TEMPLATE_PATH="gs://my-google-cloud-bucket/templates/ingestion-pipeline.json"
export TEMPLATE_IMAGE="gcr.io/$GCP_PROJECT_ID/ingestion-pipeline:latest"

gcloud dataflow flex-template build $TEMPLATE_PATH \
    --image-gcr-path "$TEMPLATE_IMAGE" \
    --sdk-language "JAVA" \
    --flex-template-base-image JAVA11 \
    --metadata-file "metadata.json" \
    --jar "target/enpa-ingestion-bundled-0.1.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline"
```

### Running the Flex Template

```sh
export SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

gcloud dataflow flex-template run "ingestion-pipeline-$USER-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters project="$PROJECT" \
    --parameters keyResourceName="$KEY_RESOURCE_NAME" \
    --parameters PHAOutput="$PHA_OUTPUT" \
    --parameters facilitatorOutput="$FACILITATOR_OUTPUT" \
    --service-account-email "$SERVICE_ACCOUNT_EMAIL" \
    --region "us-central1"
```

## Contributing

Contributions to this repository are always welcome and highly encouraged.

See [CONTRIBUTING](docs/contributing.md) for more information on how to get started.

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.

*This is not an official Google product*
