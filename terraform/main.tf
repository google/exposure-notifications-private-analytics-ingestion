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

locals {
  services = [
    "iam.googleapis.com",
    "cloudkms.googleapis.com"
  ]

  app_engine_location = (var.region == "us-central1" ? "us-central" : var.region)
  locality            = (var.locality != "" ? var.locality : replace(var.project, "enpa-ingestion-", ""))
}

resource "google_project_service" "apis" {
  for_each = toset(local.services)
  project  = var.project
  service  = each.value

  disable_dependent_services = false
  disable_on_destroy         = true
}

resource "google_storage_bucket" "bucket" {
  project = var.project
  name    = var.project

  location      = var.region
  storage_class = "STANDARD"

  # when true, all objects in the bucket will be deleted if terraform tries to
  # delete the bucket. Setting it to false is an added level of safety.
  force_destroy = false
}

resource "google_kms_key_ring" "keyring" {
  project  = var.project
  name     = "enpa-signing-key-ring"
  location = var.region

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_service.apis["cloudkms.googleapis.com"]
  ]
}

resource "google_kms_crypto_key" "key" {
  name     = "enpa-signing-key"
  key_ring = google_kms_key_ring.keyring.id
  purpose  = "ASYMMETRIC_SIGN"

  version_template {
    algorithm        = "EC_SIGN_P256_SHA256"
    protection_level = "HSM"
  }
}

data "google_kms_crypto_key_version" "key" {
  crypto_key = google_kms_crypto_key.key.id
}

resource "google_storage_bucket_object" "manifest" {
  name   = "${local.locality}-g-enpa-manifest.json"
  bucket = var.manifest_bucket

  cache_control = "no-cache,max-age=0"
  content_type  = "application/json"

  ### FIXME: our keys currently don't expire so the expiration date is just a
  ### random value I pulled out of an example file. It should be changed to
  ### something meaningful.
  content = <<-EOF
    {
      "format": 1,
      "server-identity": {
        "gcp-service-account-email": "${google_service_account.dataflow.email}",
        "gcp-service-account-id": "${google_service_account.dataflow.unique_id}"
      },
      "batch-signing-public-keys": {
        "${google_kms_crypto_key.key.id}/cryptoKeyVersions/${coalesce(data.google_kms_crypto_key_version.key.version, "0")}": {
          "public-key": "${replace(try(data.google_kms_crypto_key_version.key.public_key[0].pem, ""), "\n", "\\n")}",
          "expiration": "20211231T000000Z"
        }
      }
    }
  EOF
}

resource "google_storage_object_acl" "manifest" {
  # this needs to be output_name in order to recreate the ACL if the object is
  # recreated
  object = google_storage_bucket_object.manifest.output_name
  bucket = var.manifest_bucket

  predefined_acl = "publicRead"
}
