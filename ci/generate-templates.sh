#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

## Get the directory of the build script
scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
## cd to the parent directory, i.e. the root of the git repo
cd ${scriptDir}/..

# include common functions
source ${scriptDir}/common.sh

apk add --update npm

# Print out versions
gsutil --version
npm install -g json
json --version

export VERSION=$(git describe --tags --always --dirty=-dirty)

# Generate Dataflow Flex Templates, version and upload to GCS
json -f templates/dataflow-flex-template.json \
  -e "this.metadata=$(cat templates/dataflow-ingestion-metadata-template.json)" \
  -e "this.image='gcr.io/enpa-infra/ingestion-pipeline:$VERSION'" > ingestion-pipeline-$VERSION.json

json -f templates/dataflow-flex-template.json \
  -e "this.metadata=$(cat templates/dataflow-deletion-metadata-template.json)" \
  -e "this.image='gcr.io/enpa-infra/deletion-pipeline:$VERSION'" > deletion-pipeline-$VERSION.json

gsutil cp ingestion-pipeline-$VERSION.json gs://enpa-pipeline-specs/
gsutil cp deletion-pipeline-$VERSION.json gs://enpa-pipeline-specs/

# Version and upload scheduler templates to GCS
gsutil -h "Content-Type:application/json" cp templates/scheduler-ingestion-template.tmpl gs://enpa-pipeline-specs/scheduler-ingestion-template-$VERSION.tmpl
gsutil -h "Content-Type:application/json" cp templates/scheduler-deletion-template.tmpl gs://enpa-pipeline-specs/scheduler-deletion-template-$VERSION.tmpl

# Version Firestore Security Rules and upload to GCS
gsutil -h "Content-Type:text/plain" cp config/firebase/firestore.rules gs://enpa-infra/security-rules/firestore-$VERSION.rules
