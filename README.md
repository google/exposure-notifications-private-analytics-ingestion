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

Follow the
[Getting started with Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/dataflow/README.md)
page, and make sure you have a Google Cloud project with billing enabled
and a *service account JSON key* set up in your `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
Additionally, you also need the following:

1. [Create an asymmetric key ring](https://cloud.google.com/kms/docs/creating-asymmetric-keys)

<!-- TODO: set the roles needed for the service account -->

## Testing

### Unit Tests

To run unit tests:

```shell script
./mvnw test
```

### Integration Tests

Install the [Firebase CLI](https://firebase.google.com/docs/cli),

```shell script
npm install -g firebase-tools
```

login and setup the emulator as follows:

```sh
firebase login
firebase setup:emulators:firestore
```

Copy the Google credentials json file and store it in "credentials/google-application.json"

To run integration tests (includes standing up a Firestore emulator):

```shell script
./mvnw verify
```

## Deploying / Building DataFlow template

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

Setting the following environment variables is useful for the commands below.

```sh
FIREBASE_PROJECT_ID="my-firebase-project-id"
GCP_PROJECT_ID="my-google-cloud-ingestion-project-id"
PHA_OUTPUT="gs://my-cloud-storage-bucket/output/folder/pha"
FACILITATOR_OUTPUT="gs://my-cloud-storage-bucket/output/folder/faciliator"
KEY_RESOURCE_NAME="projects/some-ingestion-project/locations/global/keyRings/some-signature-key-ring/cryptoKeys/some-signature-key/cryptoKeyVersions/1"
METRICS="metricOfInterest1,metricOfInterest2,metricOfInterestN"
```

```sh
TEMPLATE_LOCATION="gs://my-google-cloud-bucket/templates/local-build-`date +'%Y-%m-%d-%H-%M'`"
STAGING_LOCATION="gs://my-cloud-storage-bucket/staging"

BEAM_ARGS=(
    "--metrics=$METRICS"
    "--"
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
BEAM_ARGS=(
    "--"
    "--firebaseProjectId=$FIREBASE_PROJECT_ID"
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--PHAOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
)
./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

### On Cloud

#### From local build

```sh
SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

BEAM_ARGS=(
    "--"
    "--firebaseProjectId=$FIREBASE_PROJECT_ID"
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--PHAOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
    "--runner=DataflowRunner"
    "--project=$GCP_PROJECT_ID"
    "--tempLocation=$TEMP_LOCATION"
    "--region=us-central1"
    "--serviceAccount=$SERVICE_ACCOUNT_EMAIL"
)
./mvnw compile exec:java \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

#### From previously built template

```sh
BEAM_ARGS=(
    "firebaseProjectId=$FIREBASE_PROJECT_ID"
    "keyResourceName=$KEY_RESOURCE_NAME"
    "PHAOutput=$PHA_OUTPUT"
    "facilitatorOutput=$FACILITATOR_OUTPUT"
    "deviceAttestation=false"
    "serviceAccount=$SERVICE_ACCOUNT_EMAIL"
)
gcloud dataflow jobs run "ingestion-manual-run-$USER-`date +'%Y-%m-%d-%H-%M'`" \
    --gcs-location="$TEMPLATE_LOCATION" \
    --region="us-central1" \
    --parameters="$(IFS=, eval 'echo "${BEAM_ARGS[*]}"')"
```
