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

# Print out Java
java -version

RETURN_CODE=0
set +e

git submodule update --init

retry_with_backoff 3 10 \
  ./mvnw clean verify

# enable once we can write to the firewalled sonarqube instance
#retry_with_backoff 3 10 \
#  ./mvnw -Pcoverage clean verify sonar:sonar -Dsonar.projectKey=enpa-ingestion -Dsonar.host.url=http://10.128.0.2:9000 -Dsonar.login=$SONAR_LOGIN
