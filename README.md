![Integration Test on Push](https://github.com/google/exposure-notifications-private-analytics-ingestion/workflows/Integration%20Test%20on%20Push/badge.svg?branch=main)

# Exposure Notification Private Analytics Ingestion

This repository contains batch processing jobs that can be used to ingest
private data shares according to the Exposure Notification Private Analytics
protocol. It assumes private data shares are uploaded as Firestore documents,
as is done in the
[Exposure Notification Express template app](https://github.com/google/exposure-notifications-android/blob/4b7b461282b2ede6fb2a93488c6d628440052c8d/app/src/main/java/com/google/android/apps/exposurenotification/privateanalytics/PrivateAnalyticsFirestoreRepository.java#L42).
These documents contain encrypted packets using the [Prio](https://crypto.stanford.edu/prio/)
protocol. An Apache Beam pipeline converts them into the format
that downstream Prio data processing servers expect, defined in Avro schema
[here](https://github.com/abetterinternet/prio-server/tree/master/avro-schema).

This implementation happens to make use of Firestore as a convenient way to
send up the packets to a scalable NoSQL db for subsequent batching and aggregation.
Alternative implementations might operate a custom backend endpoint to accumulate
the packets, or use a pubsub mechanism. Since the packets are encrypted on device,
the channel over which the packets travel need not be trusted.

This repository also contains Firebase configuration to lockdown Firestore
with security rules.

## Before you begin

This repository uses git submodules. You will need to initialize the git submodules 
before being able to compile.
```sh
git submodule update --init
```

Follow the
[Getting started with Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/dataflow/README.md)
page. You will need the following:

1. Set up a
    [Google Cloud project](https://console.cloud.google.com/projectcreate) or use an existing one.
    Then [import the Google Cloud project into Firebase](https://cloud.google.com/firestore/docs/client/get-firebase).

1. [Enable APIs](https://console.cloud.google.com/flows/enableapi?apiid=containerregistry.googleapis.com,cloudbuild.googleapis.com):
    Container Registry, Cloud Build, Cloud Datastore and Dataflow.

1. [Create an asymmetric key ring](https://cloud.google.com/kms/docs/creating-asymmetric-keys)

1. Create a service account with permissions for [Firestore](https://cloud.google.com/datastore/docs/access/iam#iam_roles),
    [reading the KMS key](https://cloud.google.com/kms/docs/reference/permissions-and-roles),
    and [Dataflow](https://cloud.google.com/dataflow/docs/concepts/access-control#roles).

## Environment Variables

Setting the following environment variables can be handy when working in the
project.

```sh
export PROJECT="my-google-cloud-ingestion-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="your-service-account-key.json"
export PHA_OUTPUT="gs://my-cloud-storage-bucket/output/folder/pha"
export FACILITATOR_OUTPUT="gs://my-cloud-storage-bucket/output/folder/faciliator"
export KEY_RESOURCE_NAME="projects/some-ingestion-project/locations/global/keyRings/some-signature-key-ring/cryptoKeys/some-signature-key/cryptoKeyVersions/1"
```

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
./mvnw verify
```

## Running the Pipeline

There are two pipelines. One reads Prio data shares from Firestore and
generates the outputs which the PHA and Facilitator data share processors will consume.
The other deletes expired or already processed data shares from Firestore. 

They both take as options the window of time to cover, in the form of a start
time and duration. When not supplied, start time for the ingestion pipeline is
calculated based on current time rounding back to previous window of length
`duration`. For the deletion pipeline, it goes back two windows to ensure a
safety margin of not deleting uningested data.

### Locally

To run the ingestion pipeline:

```sh
export BEAM_ARGS=(
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--phaOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
)
./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

To run the deletion pipeline:

```sh
export BEAM_ARGS=(
    "--project=$PROJECT"
)
./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.DeletionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

### On Cloud

#### From local build

```sh
export SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

export BEAM_ARGS=(
    "--keyResourceName=$KEY_RESOURCE_NAME"
    "--phaOutput=$PHA_OUTPUT"
    "--facilitatorOutput=$FACILITATOR_OUTPUT"
    "--runner=DataflowRunner"
    "--region=us-central1"
    "--serviceAccount=$SERVICE_ACCOUNT_EMAIL"
)
./mvnw compile exec:java \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline \
    -Dexec.args="$BEAM_ARGS"
```

#### From Flex Template

See [below](#creating-a-flex-template) on how to generate the flex template.


```sh
export SERVICE_ACCOUNT_EMAIL=$(egrep -o '[^"]+@[^"]+\.iam\.gserviceaccount\.com' $GOOGLE_APPLICATION_CREDENTIALS)

gcloud dataflow flex-template run "ingestion-pipeline-$USER-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters project="$PROJECT" \
    --parameters keyResourceName="$KEY_RESOURCE_NAME" \
    --parameters phaOutput="$PHA_OUTPUT" \
    --parameters facilitatorOutput="$FACILITATOR_OUTPUT" \
    --service-account-email "$SERVICE_ACCOUNT_EMAIL" \
    --region "us-central1"
```

## Building

We generate [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

### Building a Flex Template and Launch Container

To build the launch container we added profiles for the ingestion and deletion pipeline.

To build the ingestion pipeline launch container with the setting a git derived version:
```sh
./mvnw -Pingestion-container-build -Dcontainer-version=$(git describe --tags --always --dirty=-dirty) package
```

To build the deletion pipeline launch container with the setting a git derived version:
```sh
./mvnw -Pdeletion-container-build -Dcontainer-version=$(git describe --tags --always --dirty=-dirty) package
```

Containers get automatically published to your projects Google Container Registry (gcr.io)
at `gcr.io/$PROJECT_ID/ingestion-pipeline:$VERSION` and `gcr.io/$PROJECT_ID/deletion-pipeline:$VERSION`
respectively.

To generate the Flex Template Metadata files and upload them to GCS run:

*The following commands require nodejs json `npm install -g json`*

```sh
export VERSION=$(git describe --tags --always --dirty=-dirty)

json -f templates/dataflow-flex-template.json \
  -e "this.metadata=`cat templates/dataflow-ingestion-metadata-template.json`" \
  -e "this.image='gcr.io/enpa-infra/ingestion-pipeline:$VERSION'" > ingestion-pipeline-$VERSION.json

json -f templates/dataflow-flex-template.json \
  -e "this.metadata=`cat templates/dataflow-deletion-metadata-template.json`" \
  -e "this.image='gcr.io/enpa-infra/deletion-pipeline:$VERSION'" > deletion-pipeline-$VERSION.json

gsutil cp ingestion-pipeline-$VERSION.json gs://enpa-pipeline-specs/
gsutil cp deletion-pipeline-$VERSION.json gs://enpa-pipeline-specs/

gsutil -h "Content-Type:application/json" cp templates/scheduler-ingestion-template.tmpl \
  gs://enpa-pipeline-specs/scheduler-ingestion-template-$VERSION.tmpl
gsutil -h "Content-Type:application/json" cp templates/scheduler-deletion-template.tmpl \
  gs://enpa-pipeline-specs/scheduler-deletion-template-$VERSION.tmpl

unset VERSION
```

## Contributing

Contributions to this repository are always welcome and highly encouraged.

See [CONTRIBUTING](docs/contributing.md) for more information on how to get started.

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.

*This is not an official Google product*
