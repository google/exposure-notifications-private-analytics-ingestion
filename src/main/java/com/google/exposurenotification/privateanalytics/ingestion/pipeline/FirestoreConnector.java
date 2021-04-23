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
package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.firestore.v1.DatabaseRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneOffset;
import org.threeten.bp.format.DateTimeFormatter;

/**
 * Primitive beam connector for Firestore specific to this application.
 *
 * <p>For a general purpose connector see https://issues.apache.org/jira/browse/BEAM-8376
 */
public class FirestoreConnector {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreConnector.class);

  private static final long SECONDS_IN_HOUR = Duration.of(1, ChronoUnit.HOURS).getSeconds();

  // Order must be name ascending. Right now, this is the only ordering that the
  // Firestore SDK supports.
  private static final String NAME_FIELD = "__name__";

  private static final Counter queriesGenerated =
      Metrics.counter(FirestoreConnector.class, "queriesGenerated");

  /**
   * PTransfrom which will generate the necessary PartitionQueryRequests for processing documents
   */
  public static final class FirestorePartitionQueryCreation
      extends PTransform<PBegin, PCollection<PartitionQueryRequest>> {
    private final long start;

    public FirestorePartitionQueryCreation(long start) {
      this.start = start;
    }

    @Override
    public PCollection<PartitionQueryRequest> expand(PBegin input) {
      IngestionPipelineOptions options =
          (IngestionPipelineOptions) input.getPipeline().getOptions();
      LOG.info("Using start time in seconds of {}", start);
      long backwardHours = options.getGraceHoursBackwards();
      // To correctly compute how many hours forward we need to look at, when including the
      // duration, we need to compute:
      //    ceil ( forwardHours + durationInSeconds / 3600 )
      // Because Java division rounds down, we compute it as:
      //    forwardHours + ( duration + 3599 ) / 3600.
      long forwardHours =
          options.getGraceHoursForwards()
              + (options.getDuration() + (SECONDS_IN_HOUR - 1)) / SECONDS_IN_HOUR;
      LOG.info(
          "{} Querying Firestore for documents in date range: {} to {}.",
          getLogPrefix(),
          formatDateTime(start - backwardHours * SECONDS_IN_HOUR),
          formatDateTime(start + forwardHours * SECONDS_IN_HOUR));

      return input
          .apply("Begin", Create.of(generateQueries(start, backwardHours, forwardHours)))
          .apply(
              "Create PartitionQuery",
              ParDo.of(
                  new DoFn<StructuredQuery, PartitionQueryRequest>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      IngestionPipelineOptions options =
                          context.getPipelineOptions().as(IngestionPipelineOptions.class);
                      String path =
                          "".equals(options.getFirestoreProject())
                              ? getParentPath(options.getProject())
                              : getParentPath(options.getFirestoreProject());
                      PartitionQueryRequest request =
                          PartitionQueryRequest.newBuilder()
                              .setPartitionCount(options.getPartitionCount())
                              .setParent(path)
                              .setStructuredQuery(context.element())
                              .build();
                      context.output(request);
                    }
                  }));
    }
  }

  private static Iterable<StructuredQuery> generateQueries(
      long startTime, long backwardHours, long forwardHours) {
    List<StructuredQuery> structuredQueries = new ArrayList<>();
    // Each document in Firestore is stored under a Date collection with the format:
    // yyyy-MM-dd-HH.
    // To query all documents uploaded around startTime within the specified window, construct
    // a query for each hour within the window: [startTime - backwardHours, startTime +
    // forwardHours].
    for (long i = (-1 * backwardHours); i <= forwardHours; i++) {
      long timeToQuery = startTime + i * SECONDS_IN_HOUR;
      // Reformat the date to mirror the format of documents in Firestore: yyyy-MM-dd-HH.
      String formattedDateTime = formatDateTime(timeToQuery);
      // Construct and output query.
      StructuredQuery query =
          StructuredQuery.newBuilder()
              .addFrom(
                  CollectionSelector.newBuilder()
                      .setCollectionId(formattedDateTime)
                      .setAllDescendants(true)
                      .build())
              .addOrderBy(
                  Order.newBuilder()
                      .setField(FieldReference.newBuilder().setFieldPath(NAME_FIELD).build())
                      .setDirection(Direction.ASCENDING)
                      .build())
              .build();
      structuredQueries.add(query);
      queriesGenerated.inc();
    }
    LOG.info("{} Generated {} Firestore queries.", getLogPrefix(), structuredQueries.size());
    return structuredQueries;
  }

  private static String getParentPath(String projectId) {
    return DatabaseRootName.format(projectId, "(default)");
  }

  // Formats a time given in epoch seconds in the format: yyyy-MM-dd-HH
  public static String formatDateTime(Long time) {
    LocalDateTime dateTimeToQuery = LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC);
    // Reformat the date to mirror the format of documents in Firestore: yyyy-MM-dd-HH.
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH", Locale.US).withZone(ZoneOffset.UTC);
    return formatter.format(dateTimeToQuery);
  }

  // TODO: use org.slf4j.MDC (mapped diagnostic content) or something cooler here
  private static String getLogPrefix() {
    String host = "unknown";
    try {
      InetAddress address = InetAddress.getLocalHost();
      host = address.getHostName();
    } catch (UnknownHostException ignore) {
    }
    return "["
        + host
        + "|"
        + ProcessHandle.current().pid()
        + "|"
        + Thread.currentThread().getName()
        + "] - ";
  }
}
