/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.exposurenotification.privateanalytics.ingestion;

import java.util.List;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Class for accepting command flags/options that are independent of IngestionPipelineOptions.
 * Options provided to IngestionPipelineFlags can be accessed during graph construction (before
 * pipeline.run()) while options provided to IngestionPipelineOptions cannot.
 */
public class IngestionPipelineFlags {

  @Option(names = {"--metrics", "metrics"},
      split = ",",
      defaultValue = "fakeMetric-v1,histogramMetric-v1,PeriodicExposureNotificationInteraction-v1,PeriodicExposureNotification-v1",
      description = "comma-separated list of metrics to process in pipeline")
  public List<String> metrics;

  @Option(names = {"--batchSize", "batchSize"},
      defaultValue = "1000",
      description = "Batch size of individual files.")
  public Long batchSize;

  // Command options to be to be parsed for IngestionPipelineOptions
  @Parameters
  String[] pipelineOptionsParams;
}