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
  flex_template_launch_endpoint = "https://dataflow.googleapis.com/v1b3/projects/${var.project}/locations/${var.region}/flexTemplates:launch"
}

resource "google_project_service" "scheduler" {
  project = var.project
  service = "cloudscheduler.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

data "http" "ingestion_template" {
  url = "https://storage.googleapis.com/enpa-pipeline-specs/scheduler-ingestion-template-${var.pipeline_version}.tmpl"
}

data "template_file" "ingestion" {
  template = data.http.ingestion_template.body

  vars = {
    pipeline_name             = "ingestion-pipeline-${lower(replace(replace(var.pipeline_version, ".", "-"), "_", "-"))}"
    start_time                = var.ingestion_start_time
    autoscaling_algorithm     = var.ingestion_autoscaling_algorithm
    batch_size                = var.batch_size
    dev_project               = var.dev_project
    enable_device_attestation = var.enable_device_attestation
    facilitator_manifest_url  = var.facilitator_manifest_url
    key_id                    = "projects/${var.project}/locations/${var.region}/keyRings/${google_kms_key_ring.keyring.name}/cryptoKeys/${google_kms_crypto_key.key.name}/cryptoKeyVersions/1",
    machine_type              = var.ingestion_machine_type
    pha_manifest_url          = var.pha_manifest_url
    pipeline_version          = var.pipeline_version
    project                   = var.project
    region                    = var.region
    service_account           = google_service_account.dataflow.email
    temp_location             = "${google_storage_bucket.bucket.url}/temp"
    window                    = var.ingestion_window
    worker_count              = var.ingestion_worker_count
    max_worker_count          = var.ingestion_max_worker_count
    package_signature_digest  = var.package_signature_digest
    package_name              = var.package_name
  }
}

resource "google_cloud_scheduler_job" "ingestion" {
  project = var.project
  name    = "ingestion-pipeline"
  region  = var.region

  # the GCP provider currently does not support pausing/resuming scheduler jobs,
  # so if we want to disable a job the best workaround we have is to schedule it
  # far into the future. Unfortunately due to the cron format the best we can do
  # is "one year from now", where "now" means the time at which I'm typing this
  # comment.
  #
  # Since we don't expect this project to live for another year it should be
  # fine, but don't be surprised if your pipeline runs at noon UTC on December
  # 15th.
  schedule  = (var.enable_pipelines ? var.ingestion_schedule : "0 12 15 12 *")
  time_zone = "Etc/UTC"

  http_target {
    oauth_token {
      service_account_email = google_service_account.dataflow.email
    }

    http_method = "POST"
    uri         = local.flex_template_launch_endpoint
    body        = base64encode(data.template_file.ingestion.rendered)
  }

  depends_on = [
    google_project_service.scheduler
  ]
}

data "http" "deletion_template" {
  url = "https://storage.googleapis.com/enpa-pipeline-specs/scheduler-deletion-template-${var.pipeline_version}.tmpl"
}

data "template_file" "deletion" {
  template = data.http.deletion_template.body

  vars = {
    pipeline_name         = "deletion-pipeline-${lower(replace(replace(var.pipeline_version, ".", "-"), "_", "-"))}"
    start_time            = var.deletion_start_time
    autoscaling_algorithm = var.deletion_autoscaling_algorithm
    machine_type          = var.deletion_machine_type
    pipeline_version      = var.pipeline_version
    service_account       = google_service_account.dataflow.email
    window                = var.deletion_window
    worker_count          = var.deletion_worker_count
    max_worker_count      = var.deletion_max_worker_count
  }
}

resource "google_cloud_scheduler_job" "deletion" {
  project = var.project
  name    = "deletion-pipeline"
  region  = var.region

  # see comment in the ingestion job definition for info about this magic value
  schedule  = (var.enable_pipelines ? var.deletion_schedule : "0 12 15 12 *")
  time_zone = "Etc/UTC"

  http_target {
    oauth_token {
      service_account_email = google_service_account.dataflow.email
    }

    http_method = "POST"
    uri         = local.flex_template_launch_endpoint
    body        = base64encode(data.template_file.deletion.rendered)
  }

  depends_on = [
    google_project_service.scheduler
  ]
}
