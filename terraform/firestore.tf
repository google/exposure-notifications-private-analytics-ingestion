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
  temp_dir = "${path.module}/.terraform/tmp"
}

data "google_storage_object_signed_url" "firestore_rules" {
  bucket   = "enpa-infra"
  path     = "security-rules/firestore-${var.pipeline_version}.rules"
  duration = "5m"
}

data "http" "firestore_rules" {
  url = data.google_storage_object_signed_url.firestore_rules.signed_url
}

resource "local_file" "firestore_rules" {
  filename = "${local.temp_dir}/firestore.rules"

  # the content is not really sensitive in the normal sense, it's just enormous
  # and easier to elide than to scroll through.
  sensitive_content = data.http.firestore_rules.body

  file_permission      = "0644"
  directory_permission = "0755"
}

resource "local_file" "firebase_json" {
  filename = "${local.temp_dir}/firebase.json"
  content  = jsonencode({ firestore = { rules = "firestore.rules" } })

  file_permission      = "0644"
  directory_permission = "0755"
}

resource "null_resource" "firestore_security_rules" {
  triggers = {
    # if either of the config files changes, upload the rules
    config = local_file.firebase_json.content
    rules  = local_file.firestore_rules.content

    # if the version changes upload the rules even if the files didn't change
    version = var.pipeline_version
  }

  provisioner "local-exec" {
    command     = "firebase deploy --only firestore:rules --project ${var.project}"
    working_dir = local.temp_dir

    environment = {
      GOOGLE_APPLICATION_CREDENTIALS = "${abspath(path.root)}/credentials.json"
    }
  }
}
