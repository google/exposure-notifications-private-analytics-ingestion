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

### Required

variable "project" {
  type        = string
  description = "The ID of the Google Cloud project created previously. Required."
}

variable "project_owners_list" {
  type        = list(string)
  description = "The list of fully-qualified owners (user:<user-email>, group:<group-email>, serviceAccount:<svc-acct-email>) of the project"
}

variable "pipeline_version" {
  type        = string
  description = "Dataflow tuning parameter: the version of the pipeline code. Required."

}

variable "dev_project" {
  type        = bool
  description = "Should this project be configured for development? Defaults to false."
  default     = false
}

# should only be set if dev_project is false
variable "facilitator_manifest_url" {
  type        = string
  description = "The facilitator manifest URL"
  default     = ""
}

# should only be set if dev_project is false
variable "pha_manifest_url" {
  type        = string
  description = "The PHA manifest URL"
  default     = ""
}

### Suggested

variable "region" {
  type        = string
  description = "The Google Cloud region in which to create regional resources. Defaults to us-central1."
  default     = "us-central1"
}

variable "locality" {
  type        = string
  description = "The locality string of the ENPA installation. Optional to override locality setting."
  default     = ""
}

variable "enable_device_attestation" {
  type        = bool
  description = "Dataflow tuning parameter: whether to enable device attestation. Defaults to true."
  default     = true
}

variable "ingestion_schedule" {
  type        = string
  description = "a string describing the schedule for ingestion jobs, in cron format. Default: '30 * * * *' (30 minutes past each hour)"
  default     = "30 * * * *"
}

variable "ingestion_window" {
  type        = number
  description = "Dataflow tuning parameter: the length (in seconds) of the window that the ingestion pipeline will use to look for new records. Defaults to one hour (3600 seconds)."
  default     = 3600
}

variable "deletion_schedule" {
  type        = string
  description = "a string describing the schedule for deletion jobs, in cron format. Default: '0 6,18 * * *' (0600 and 1800, UTC)"
  default     = "0 6,18 * * *"
}

variable "deletion_window" {
  type        = number
  description = "Dataflow tuning parameter: the length (in seconds) of the window that the deletion pipeline will use to look for records. Defaults to twelve hours (43200 seconds)."
  default     = 43200
}

### Pipeline Tuning

variable "batch_size" {
  type        = number
  description = "Dataflow tuning parameter: the number of records per batch. Defaults to 100,000."
  default     = 100000
}
variable "ingestion_start_time" {

  type        = number
  description = "Start time in UTC seconds of documents to process for the ingestion pipeline. Defaults to 0 (not set)"
  default     = 0
}

variable "ingestion_machine_type" {
  type        = string
  description = "Dataflow tuning parameter: the type of machine to use for the ingestion pipeline. Defaults to n1-standard-4."
  default     = "n1-standard-4"
}

variable "ingestion_worker_count" {
  type        = number
  description = "Dataflow tuning parameter: the number of workers used by the ingestion pipeline. Defaults to 10."
  default     = 10
}

variable "ingestion_max_worker_count" {
  type        = number
  description = "Dataflow tuning parameter: the number of maximum workers used by the ingestion pipeline. Defaults to 15."
  default     = 15
}

variable "ingestion_autoscaling_algorithm" {
  type        = string
  description = "Dataflow tuning parameter: the autoscaling algorithm used by the ingestion pipeline. Can be either THROUGHPUT_BASED or NONE. Defaults to NOT SET."
  default     = ""
}

variable "package_signature_digest" {
  type        = string
  description = "Android package signature digest to use during certificate checking. Defaults to NOT SET"
  default     = ""
}

variable "package_name" {
  type        = string
  description = "Android package name to use during certificate checking. Defaults to NOT SET"
  default     = ""
}

variable "deletion_start_time" {

  type        = number
  description = "Start time in UTC seconds of documents to process for the deletion pipeline. Defaults to 0 (not set)"
  default     = 0
}

variable "deletion_machine_type" {
  type        = string
  description = "Dataflow tuning parameter: the type of machine to use for the ingestion pipeline. Defaults to n1-standard-2."
  default     = "n1-standard-2"
}

variable "deletion_worker_count" {
  type        = number
  description = "Dataflow tuning parameter: the number of workers used by the deletion pipeline. Defaults to 10."
  default     = 10
}

variable "deletion_max_worker_count" {
  type        = number
  description = "Dataflow tuning parameter: the number of maximum workers used by the deletion pipeline. Defaults to 20."
  default     = 20
}

variable "deletion_autoscaling_algorithm" {
  type        = string
  description = "Dataflow tuning parameter: the autoscaling algorithm used by the deletion pipeline. Can be either THROUGHPUT_BASED or NONE. Defaults to NOT SET."
  default     = ""
}

### Internals

variable "enable_pipelines" {
  type        = bool
  description = "Whether to enable the scheduling of dataflow pipelines. Defaults to true."
  default     = true
}

variable "manifest_bucket" {
  type        = string
  description = "The bucket in which to store the generated manifest. Defaults to 'prio-manifests'."
  default     = "prio-manifests"
}

variable "templates_bucket" {
  type        = string
  description = "The bucket in which templates are fetched from. Defaults to 'enpa-infra'."
  default     = "enpa-infra"
}

variable "terraform_svc_account_email" {
  type        = string
  description = "The email address of the Terraform Runner service account"
}