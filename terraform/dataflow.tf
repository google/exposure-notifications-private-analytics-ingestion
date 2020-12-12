#
# Copyright 2020, Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

resource "google_service_account" "dataflow" {
  project    = var.project
  account_id = "dataflow-job-runner"

  display_name = "Dataflow Job Runner"
  description  = "Service account for dataflow pipelines"
}

resource "google_project_service" "dataflow" {
  project = var.project
  service = "dataflow.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_iam_member" "dataflow" {
  project = var.project
  role    = "roles/${each.value}"
  member  = "serviceAccount:${google_service_account.dataflow.email}"

  ### FIXME: these roles are almost certainly overly broad. We should create a
  ### custom role that grants only the permissions required.
  for_each = toset([
    "cloudkms.signer",
    "containerregistry.ServiceAgent",
    "dataflow.admin",
    "dataflow.developer",
    "dataflow.worker",
    "datastore.user",
    "editor",
    "iam.serviceAccountUser",
  ])
}
