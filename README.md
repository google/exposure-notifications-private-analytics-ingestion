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
Copy the Google credentials json file and store it in "credentials/google-application.json"

To run integration tests (includes standing up a Firestore emulator):

```shell script
mvn verify
```

## Deploying / Building DataFlow template

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

Set the following environment variables are useful for the commands below.

```shell script
FIREBASE_PROJECT_ID=firebase-project-id
GCP_PROJECT_ID=some-ingestion-project-id
PHA_OUTPUT=gs://some/output/folder/pha
FACILITATOR_OUTPUT=gs://some/output/folder/faciliator
KEY_RESOURCE_NAME=projects/some-ingestion-project/locations/global/keyRings/some-signature-key-ring/cryptoKeys/some-signature-key/cryptoKeyVersions/1
```


```shell script
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--project=$GCP_PROJECT_ID --runner=DataflowRunner --region=us-central1 --stagingLocation=$STAGING --templateLocation=gs://$TEMPLATE_BUCKET/templates/local-build-"`date +'%Y-%m-%d-%H-%M'`
```

## Running the pipeline

### Locally

```shell script
mvn -Pdirect-runner compile exec:java -Djava.util.logging.config.file=logging.properties -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline -Dexec.args="--PHAOutput=$PHA_OUTPUT --facilitatorOutput=$FACILITATOR_OUTPUT --firebaseProjectId=$FIREBASE_PROJECT_ID --keyResourceName=$KEY_RESOURCE_NAME"
```

### On Cloud

#### From local build

```shell script
mvn -Pdataflow-runner compile exec:java  -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline  -Dexec.args="--project=$GCP_PROJECT_ID --stagingLocation=$STAGING_LOCATION --runner=DataflowRunner --region=us-central1 --PHAOutput=$PHA_OUTPUT --facilitatorOutput=$FACILITATOR_OUTPUT --firebaseProjectId=$FIREBASE_PROJECT_ID --keyResourceName=$KEY_RESOURCE_NAME"
```

#### From previously built template

```shell script
gcloud dataflow jobs run "ingestion-manual-run-${USER}-"`date +'%Y-%m-%d-%H-%M'` --gcs-location=$TEMPLATE_LOCATION --region=us-central1 --parameters="PHAOutput=$PHA_OUTPUT,facilitatorOutput=$FACILITATOR_OUTPUT,firebaseProjectId=$FIREBASE_PROJECT_ID,deviceAttestation=false,keyResourceName=$KEY_RESOURCE_NAME" 
```
