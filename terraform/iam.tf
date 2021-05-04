#
# Copyright 2021, Google LLC.
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

resource "google_project_iam_custom_role" "terraform_service_account_role" {
  project = var.project
  role_id = "ENPATerraformRunner"
  title   = "Terraform Runner for ENPA Infra provisioning"
  permissions = [
    "cloudkms.cryptoKeyVersions.create",
    "cloudkms.cryptoKeyVersions.destroy",
    "cloudkms.cryptoKeyVersions.get",
    "cloudkms.cryptoKeyVersions.viewPublicKey",
    "cloudkms.cryptoKeys.create",
    "cloudkms.cryptoKeys.get",
    "cloudkms.cryptoKeys.update",
    "cloudkms.keyRings.create",
    "cloudkms.keyRings.get",
    "cloudscheduler.jobs.create",
    "cloudscheduler.jobs.delete",
    "cloudscheduler.jobs.get",
    "firebase.projects.get",
    "firebaserules.releases.list",
    "firebaserules.releases.update",
    "firebaserules.rulesets.create",
    "firebaserules.rulesets.delete",
    "firebaserules.rulesets.get",
    "firebaserules.rulesets.test",
    "iam.serviceAccounts.actAs",
    "iam.serviceAccounts.create",
    "iam.serviceAccounts.delete",
    "iam.serviceAccounts.get",
    "resourcemanager.projects.get",
    "resourcemanager.projects.getIamPolicy",
    "resourcemanager.projects.setIamPolicy",
    "serviceusage.operations.get",
    "serviceusage.services.disable",
    "serviceusage.services.enable",
    "serviceusage.services.get",
    "serviceusage.services.list",
    "storage.buckets.create",
    "storage.buckets.delete",
    "storage.buckets.get",
  ]
}

resource "google_project_iam_member" "terraform_service_account_permissions" {
  project = var.project
  role    = "projects/${var.project}/roles/${google_project_iam_custom_role.terraform_service_account_role.role_id}"
  member  = "serviceAccount:${var.terraform_svc_account_email}"
}

resource "google_project_iam_binding" "owners" {
  project = var.project
  role    = "roles/owner"
  members = var.project_owners_list
}