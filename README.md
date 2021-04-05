![Integration Test on Push](https://github.com/google/exposure-notifications-private-analytics-ingestion/workflows/Integration%20Test%20on%20Push/badge.svg?branch=main)

[![ci](https://github.com/google/exposure-notifications-private-analytics-ingestion/actions/workflows/ci.yaml/badge.svg)](https://github.com/google/exposure-notifications-private-analytics-ingestion/actions/workflows/ci.yaml)

# Exposure Notification Private Analytics Ingestion

This repository contains implementations for [Apache Beam](https://beam.apache.org/)
batch pipelines to process private data shares stored in Firestore according
to the Exposure Notification Private Analytics protocol. It assumes private data
shares uploaded as Firestore documents, as done in the
[Exposure Notification Express template app](https://github.com/google/exposure-notifications-android/blob/4b7b461282b2ede6fb2a93488c6d628440052c8d/app/src/main/java/com/google/android/apps/exposurenotification/privateanalytics/PrivateAnalyticsFirestoreRepository.java#L42).
These documents contain encrypted packets using the [Prio](https://crypto.stanford.edu/prio/)
protocol. The pipeline implementation converts them into the format that
downstream Prio data processing servers expect, defined in the Avro schema
[here](https://github.com/abetterinternet/prio-server/tree/master/avro-schema).

This implementation make use of Firestore as a scalable NoSQL db for subsequent
batching and aggregation. Alternative implementations might operate a custom
backend endpoint to accumulate the packets, or use a pubsub mechanism. Since the
packets are encrypted on device, the channel over which the packets travel need
not be trusted.

This repository also contains the Firebase configuration to secure Firestore
with [security rules](config/firebase/firestore.rules) as well as
[Terraform scripts](terraform/main.tf) to bring up required infrastructure.

## Setup

### Multiple Maven modules

The project is structured into multiple maven modules to allow incorporation of
outside implementations of attestation. Implementations need only depend on the
DataShare model module, and a profile can be added to get it included in the
pipeline module build. The pipeline pulls available implementations dynamically.

Since there aren't too many individual classes that make up each module, and
since they are only meant to be packaged and executed together, we use a single
source tree for all modules.

Follow the
[Getting started with Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/dataflow/README.md)
page. You will need the following:

1. Set up a
    [Google Cloud project](https://console.cloud.google.com/projectcreate) or use an existing one.
    Then [import the Google Cloud project into Firebase](https://cloud.google.com/firestore/docs/client/get-firebase).

1. [Enable APIs](https://console.cloud.google.com/flows/enableapi?apiid=containerregistry.googleapis.com,cloudbuild.googleapis.com):
    Container Registry, Cloud Build, Cloud Datastore and Dataflow.

1. [Create an asymmetric key](https://cloud.google.com/kms/docs/creating-asymmetric-keys)

1. [Create a storage bucket](https://cloud.google.com/storage/docs/creating-buckets)
    for your outputs.

1. Create a service account with permissions for [Firestore](https://cloud.google.com/datastore/docs/access/iam#iam_roles),
    [reading the KMS key](https://cloud.google.com/kms/docs/reference/permissions-and-roles),
    [Dataflow](https://cloud.google.com/dataflow/docs/concepts/access-control#roles),
    and [Cloud Storage](https://cloud.google.com/storage/docs/access-control/iam).

1. Download the a key for your service account and store as `credentials.json`.
    Keep those credentials save!

### Useful Environment Variables

Setting the following environment variables can be handy when working in the
project. Replace values in `[...]`.

```sh
export PROJECT="[my-google-cloud-ingestion-project-id]"
export GOOGLE_APPLICATION_CREDENTIALS="credentials.json"
export TEMPLATES="gs://[my-cloud-storage-bucket]/templates"
export PHA_OUTPUT="gs://[my-cloud-storage-bucket]/output/pha"
export FACILITATOR_OUTPUT="gs://[my-cloud-storage-bucket]/output/faciliator"
export KEY_RESOURCE_NAME="projects/[some-ingestion-project]/locations/global/keyRings/[some-signature-key-ring]/cryptoKeys/[some-signature-key]/cryptoKeyVersions/1"
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
generates the outputs which the PHA and Facilitator data share processors will
consume. The other deletes expired or already processed data shares from
Firestore.

They both take as options the window of time to cover, in the form of a start
time and duration. When not supplied, start time for the ingestion pipeline is
calculated based on current time rounding back to previous window of length
`duration`. For the deletion pipeline, it goes back two windows to ensure a
safety margin of not deleting unprocessed data shares.

### Locally

To run the ingestion pipeline locally:

```sh

./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.pipeline.IngestionPipeline \
    -Dexec.args="--keyResourceName=$KEY_RESOURCE_NAME --phaOutput=$PHA_OUTPUT --facilitatorOutput=$FACILITATOR_OUTPUT"
```

To run the deletion pipeline:

```sh
./mvnw compile exec:java \
    -Djava.util.logging.config.file=logging.properties \
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.pipeline.DeletionPipeline \
    -Dexec.args="--project=$PROJECT"
```

### In Google Cloud Dataflow

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
    -Dexec.mainClass=com.google.exposurenotification.privateanalytics.ingestion.pipeline.IngestionPipeline \
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

We generate a [templated dataflow job](https://cloud.google.com/dataflow/docs/guides/templates/overview#templated-dataflow-jobs)
that takes all pipeline options as runtime parameters.

### Building a Flex Template and Launch Container

To build the launch container we added profiles for the ingestion and deletion pipeline.

To build the ingestion pipeline launch container with setting a git derived version:

```sh
./mvnw -Pingestion-container-build -Dcontainer-version=$(git describe --tags --always --dirty=-dirty) \
-Dcontainer_registry_tag_prefix='gcr.io/[YOUR_CLOUD_PROJECT]' package
```

To build the ingestion pipeline with a custom attestation implementation,
include the additional `attestation` profile, which assumes the package is
available in any of your configured maven repositories
(in .m2/settings.xml or local mvn-settings.xml):

```sh
./mvnw -Pingestion-container-build,attestation -Dcontainer-version=$(git describe --tags --always --dirty=-dirty) \
-Dcontainer_registry_tag_prefix='gcr.io/[YOUR_CLOUD_PROJECT]' package
```

To build the deletion pipeline launch container with the setting a git derived version:

```sh
./mvnw -Pdeletion-container-build -Dcontainer-version=$(git describe --tags --always --dirty=-dirty) \
-Dcontainer_registry_tag_prefix='gcr.io/[YOUR_CLOUD_PROJECT]' package
```

Built containers get automatically published to the `container_registry_tag_prefix` you set. E.g. for Google container
registry: `gcr.io/[YOUR_CLOUD_PROJECT]/ingestion-pipeline:$VERSION` and `gcr.io/[YOUR_CLOUD_PROJECT]/deletion-pipeline:$VERSION`
respectively.

To generate the Flex Template Metadata files and upload them to GCS run:

*The following commands require nodejs json `npm install -g json`* 
Use the same `container_registry_tag_prefix` as in the builds above.

```sh
export VERSION=$(git describe --tags --always --dirty=-dirty)

json -f templates/dataflow-flex-template.json \
  -e "this.metadata=`cat templates/dataflow-ingestion-metadata-template.json`" \
  -e "this.image='gcr.io/[YOUR_CLOUD_PROJECT]/ingestion-pipeline:$VERSION'" > ingestion-pipeline-$VERSION.json

json -f templates/dataflow-flex-template.json \
  -e "this.metadata=`cat templates/dataflow-deletion-metadata-template.json`" \
  -e "this.image='gcr.io/[YOUR_CLOUD_PROJECT]/deletion-pipeline:$VERSION'" > deletion-pipeline-$VERSION.json

gsutil cp ingestion-pipeline-$VERSION.json $TEMPLATES
gsutil cp deletion-pipeline-$VERSION.json $TEMPLATES

gsutil -h "Content-Type:application/json" cp templates/scheduler-ingestion-template.tmpl \
  $TEMPLATES/scheduler-ingestion-template-$VERSION.tmpl
gsutil -h "Content-Type:application/json" cp templates/scheduler-deletion-template.tmpl \
  $TEMPLATES/scheduler-deletion-template-$VERSION.tmpl

unset VERSION
```

## Contributing

Contributions to this repository are always welcome and highly encouraged.

See [CONTRIBUTING](docs/contributing.md) for more information on how to get started.

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.

*This is not an official Google product*
