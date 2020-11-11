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

import static com.google.common.truth.Truth.assertThat;

import java.net.URL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataProcessorManifest}. */
@RunWith(JUnit4.class)
public class DataProcessorManifestTest {

  @Test
  public void testParsing() {
    URL manifestUrl =
        getClass()
            .getResource(
                "/java/com/google/exposurenotification/privateanalytics/ingestion/test-manifest.json");
    DataProcessorManifest manifest = new DataProcessorManifest(manifestUrl.toString());
    assertThat(manifest.getIngestionBucket())
        .isEqualTo("s3://us-west-1/prio-demo-gcp-test-pha-1-ingestor-1-ingestion");
  }
}
