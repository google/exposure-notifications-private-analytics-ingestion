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

1. Set up a [Firebase project](https://firebase.google.com/) and a
    [Google Cloud project](https://console.cloud.google.com/projectcreate) or use existing ones.

    ```sh
    export FIREBASE_PROJECT_ID="my-firebase-project-id"
    export GCP_PROJECT_ID="my-google-cloud-ingestion-project-id"
    ```

1. [Create an asymmetric key ring](https://cloud.google.com/kms/docs/creating-asymmetric-keys)

    ```sh
    export KMS_KEYRING="my-keyring"
    export KMS_KEY="my-key"

    export KMS_KEY_ID=$(gcloud --project $GCP_PROJECT_ID kms keys list --location global --keyring $KMS_KEYRING --filter $KMS_KEY --format "value(NAME)")
    ```

1. Specify a path prefix for the PHA and Facilitator output files.

    ```sh
    export PHA_OUTPUT="gs://my-cloud-storage-bucket/output/folder/pha"
    export FACILITATOR_OUTPUT="gs://my-cloud-storage-bucket/output/folder/faciliator"
    ```

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

## Running the pipeline

### Locally

```sh
BEAM_ARGS=(
    "--firebaseProjectId=$FIREBASE_PROJECT_ID"
    "--keyResourceName=$KMS_KEY_ID"
    "--PHAOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
)
./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

### Dataflow in Google Cloud

```sh
export TEMP_LOCATION="gs://my-google-cloud-bucket/temp"
export SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

BEAM_ARGS=(
    "--firebaseProjectId=$FIREBASE_PROJECT_ID"
    "--keyResourceName=$KMS_KEY_ID"
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
