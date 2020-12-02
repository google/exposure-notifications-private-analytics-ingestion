// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

/**
 * Encapsulation of the specific manifest for a PHA or Facilitator data processor.
 *
 * <p>See
 * https://docs.google.com/document/d/1MdfM3QT63ISU70l63bwzTrxr93Z7Tv7EDjLfammzo6Q/edit#bookmark=id.8skgn5yx33ae
 * https://github.com/abetterinternet/prio-server/blob/main/manifest-updater/manifest/types.go
 */
public class DataProcessorManifest {

  private static final String AWS_BUCKET_PREFIX = "s3://";

  private static final String INGESTION_BUCKET = "ingestion-bucket";
  private static final String INGESTION_IDENTITY = "ingestion-identity";

  private final String manifestUrl;

  private String bucket;

  private String awsBucketRegion;

  private String awsBucketName;

  private String awsRole;

  public DataProcessorManifest(String manifestUrl) {
    this.manifestUrl = manifestUrl;
    if (!"".equals(manifestUrl)) {
      init();
    }
  }

  public String getIngestionBucket() {
    return bucket;
  }

  public String getAwsBucketRegion() {
    return awsBucketRegion;
  }

  public String getAwsBucketName() {
    return awsBucketName;
  }

  public String getAwsRole() {
    return awsRole;
  }

  private void init() {
    try {
      JsonObject manifestJson = fetchAndParseJson();
      bucket = manifestJson.get(INGESTION_BUCKET).getAsString();

      if (bucket.startsWith(AWS_BUCKET_PREFIX)) {
        String bucketInfo = bucket.substring(AWS_BUCKET_PREFIX.length());
        String[] regionName = bucketInfo.split("/");
        if (regionName.length != 2) {
          throw new IllegalArgumentException(
              "Ingestion bucket not in correct format of {AWS region}/{name}");
        }

        awsBucketRegion = regionName[0];
        awsBucketName = regionName[1];
        if (manifestJson.get(INGESTION_IDENTITY) == null) {
          throw new IllegalArgumentException(
              "Ingestion identity must be specified with AWS buckets");
        } else {
          awsRole = manifestJson.get(INGESTION_IDENTITY).getAsString();
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Unable to fetch and parse manifest", e);
    }
  }

  private JsonObject fetchAndParseJson() throws IOException {
    URL url = new URL(manifestUrl);
    InputStreamReader manifestReader = new InputStreamReader(url.openStream());
    return new JsonParser().parse(manifestReader).getAsJsonObject();
  }
}
