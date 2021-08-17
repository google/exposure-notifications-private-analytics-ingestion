#!/bin/bash
# Copyright 2021 Google LLC
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

## Run Firebase integration tests
echo "************ Running Firebase integration tests script ************"
## Get the directory of the build script
scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
## cd to the parent directory, i.e. the root of the git repo
cd ${scriptDir}/..
cd config/firebase

echo "************ Installing npm testing library and jest ************"
npm init -y
npm i @firebase/testing jest
echo "************ Dependencies installed successfully! ************"

echo "************ Executing rules.test.js ************"
firebase emulators:exec --project=$PROJECT --only firestore "npm run test"
