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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.InvalidDataShareException;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.Write;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneOffset;
import org.threeten.bp.format.DateTimeFormatter;

/**
 * Primitive beam connector for Firestore native specific to ENPA.
 *
 * <p>For a general purpose connector see https://issues.apache.org/jira/browse/BEAM-8376
 */
public class FirestoreConnector {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreConnector.class);

  private static final long SECONDS_IN_HOUR = Duration.of(1, ChronoUnit.HOURS).getSeconds();

  // Order must be name ascending. Right now, this is the only ordering that the
  // Firestore SDK supports.
  private static final String NAME_FIELD = "__name__";

  private static final Duration FIRESTORE_WAIT_TIME = Duration.ofSeconds(30);

  private static final Counter queriesGenerated =
      Metrics.counter(FirestoreConnector.class, "queriesGenerated");

  private static final Counter partitionCursors =
      Metrics.counter(FirestoreConnector.class, "partitionCursors");

  private static final Counter dataShares = Metrics.counter(FirestoreConnector.class, "dataShares");

  private static final Counter invalidDocumentCounter =
      Metrics.counter(FirestoreConnector.class, "invalidDocuments");

  private static final Counter grpcException =
      Metrics.counter(FirestoreConnector.class, "grpcException");

  private static final Counter documentsRead =
      Metrics.counter(FirestoreConnector.class, "documentsRead");

  private static final Counter documentsDeleted =
      Metrics.counter(FirestoreConnector.class, "documentsDeleted");

  private static final Counter failedDeletes =
      Metrics.counter(FirestoreConnector.class, "failedDeletes");

  private static final Counter skippedResults =
      Metrics.counter(FirestoreConnector.class, "skippedResults");

  private static final Counter partialProgress =
      Metrics.counter(FirestoreConnector.class, "partialProgress");

  /** Reads documents from Firestore */
  public static final class FirestoreReader extends PTransform<PBegin, PCollection<DataShare>> {

    @Override
    public PCollection<DataShare> expand(PBegin input) {
      IngestionPipelineOptions options =
          (IngestionPipelineOptions) input.getPipeline().getOptions();
      long backwardWindow = options.getGracePeriodBackwards() / SECONDS_IN_HOUR;
      long forwardWindow =
          (options.getDuration() + options.getGracePeriodForwards()) / SECONDS_IN_HOUR;
      return input
          .apply(
              "Begin",
              Create.of(generateQueries(options.getStartTime(), backwardWindow, forwardWindow)))
          .apply("PartitionQuery", ParDo.of(new PartitionQueryFn()))
          .apply("Read", ParDo.of(new ReadFn()))
          // In case workers retried on some shards and duplicates got emitted, ensure distinctness
          .apply(
              Distinct.<DataShare, String>withRepresentativeValueFn(
                  // Not using a lambda here as Beam has trouble inferring a coder
                  new SerializableFunction<DataShare, String>() {
                    @Override
                    public String apply(DataShare dataShare) {
                      return dataShare.getPath();
                    }
                  }));
    }

    private Iterable<StructuredQuery> generateQueries(
        long startTime, long backwardWindow, long forwardWindow) {
      List<StructuredQuery> structuredQueries = new ArrayList<>();
      // Each datashare in Firestore is stored under a Date collection with the format:
      // yyyy-MM-dd-HH.
      // To query all documents uploaded around startTime within the specified window, construct
      // a query for each hour within the window: [startTime - backwardWindow, startTime +
      // forwardWindow].
      for (long i = (-1 * backwardWindow); i <= forwardWindow; i++) {
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
      return structuredQueries;
    }

    static class PartitionQueryFn
        extends DoFn<StructuredQuery, ImmutableTriple<Cursor, Cursor, StructuredQuery>> {

      private FirestoreClient client;

      @StartBundle
      public void startBundle() throws Exception {
        client = getFirestoreClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        IngestionPipelineOptions options =
            context.getPipelineOptions().as(IngestionPipelineOptions.class);
        PartitionQueryRequest request =
            PartitionQueryRequest.newBuilder()
                .setPartitionCount(options.getPartitionCount())
                .setParent(getParentPath(options.getFirebaseProjectId()))
                .setStructuredQuery(context.element())
                .build();
        PartitionQueryPagedResponse response = client.partitionQuery(request);
        Iterator<Cursor> iterator = response.iterateAll().iterator();

        // Return a Cursor pair to represent the start and end points within which to run the query.
        Cursor start = null;
        Cursor end;
        while (iterator.hasNext()) {
          end = iterator.next();
          context.output(ImmutableTriple.of(start, end, context.element()));
          partitionCursors.inc();
          start = end;
        }
        context.output(ImmutableTriple.of(start, null, context.element()));
        partitionCursors.inc();
      }

      @FinishBundle
      public void finishBundle() {
        shutdownFirestoreClient(client);
      }
    }

    static class ReadFn extends DoFn<ImmutableTriple<Cursor, Cursor, StructuredQuery>, DataShare> {

      private FirestoreClient client;

      @StartBundle
      public void startBundle() throws Exception {
        client = getFirestoreClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        IngestionPipelineOptions options =
            context.getPipelineOptions().as(IngestionPipelineOptions.class);
        for (DataShare ds :
            readDocumentsFromFirestore(client, options.getFirebaseProjectId(), context.element())) {
          context.output(ds);
          dataShares.inc();
        }
      }

      @FinishBundle
      public void finishBundle() {
        shutdownFirestoreClient(client);
      }
    }
  }

  // TODO pull Deleter out of FirestoreConnector to be run as its own job.
  /** Deletes documents from Firestore. */
  public static final class FirestoreDeleter extends PTransform<PCollection<DataShare>, PDone> {

    @Override
    public PDone expand(PCollection<DataShare> input) {
      // TODO: would it be useful to sort on document paths to get more efficient deletes?
      IngestionPipelineOptions options =
          input.getPipeline().getOptions().as(IngestionPipelineOptions.class);
      Long deleteBatchSize = options.getDeleteBatchSize();
      PCollection<KV<Long, Iterable<DataShare>>> dataSharesGroupedIntoBatches =
          input
              .apply(
                  "AddRandomLongAsKey",
                  MapElements.via(
                      new SimpleFunction<DataShare, KV<Long, DataShare>>() {
                        @Override
                        public KV<Long, DataShare> apply(DataShare input) {
                          return KV.of(
                              ThreadLocalRandom.current().nextLong(0L, deleteBatchSize), input);
                        }
                      }))
              .apply("GroupIntoBatches", GroupIntoBatches.ofSize(deleteBatchSize));
      dataSharesGroupedIntoBatches.apply("BatchDelete", ParDo.of(new DeleteFn()));
      return PDone.in(input.getPipeline());
    }

    static class DeleteFn extends DoFn<KV<Long, Iterable<DataShare>>, Void> {

      private FirestoreClient client;

      @StartBundle
      public void startBundle() throws Exception {
        client = getFirestoreClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        IngestionPipelineOptions options =
            context.getPipelineOptions().as(IngestionPipelineOptions.class);
        // TODO: if this is the last document in the date subcollection, the date subcollection will
        // be deleted.
        //  If the date subcollection is the last element in its parent document, that document
        // should also be deleted.
        if (options.getDelete() && context.element().getValue() != null) {
          BatchWriteRequest.Builder batchDelete =
              BatchWriteRequest.newBuilder()
                  .setDatabase(
                      "projects/" + options.getFirebaseProjectId() + "/databases/(default)");
          for (DataShare ds : context.element().getValue()) {
            if (ds.getPath() == null) {
              LOG.warn("Attempted to delete DataShare with null Path");
            }
            batchDelete.addWrites(Write.newBuilder().setDelete(ds.getPath()).build());
          }
          BatchWriteResponse response = client.batchWrite(batchDelete.build());
          List<Status> deleteResults = response.getStatusList();
          for (int index = 0; index < deleteResults.size(); index++) {
            Status status = deleteResults.get(index);
            if (status.getCode() != Code.OK.getNumber()) {
              failedDeletes.inc();
              LOG.warn(
                  "Failed to delete doc at: %s with Code: %s and Message: %s",
                  batchDelete.getWrites(index).getDelete(), status.getCode(), status.getMessage());
            } else {
              documentsDeleted.inc();
            }
          }
        }
      }

      @FinishBundle
      public void finishBundle() throws InterruptedException {
        shutdownFirestoreClient(client);
      }
    }
  }

  // Returns a v1.Firestore instance to be used to partition read queries.
  private static FirestoreClient getFirestoreClient() throws IOException {
    FirestoreSettings settings =
        FirestoreSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
            .build();
    return FirestoreClient.create(settings);
  }

  public static void shutdownFirestoreClient(FirestoreClient client) {
    client.shutdown();
    LOG.info("Waiting for FirestoreClient to shutdown.");
    try {
      client.awaitTermination(FIRESTORE_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for client shutdown", e);
    }
  }

  private static String getParentPath(String projectId) {
    return "projects/" + projectId + "/databases/(default)/documents";
  }

  // Formats a time given in epoch seconds in the format: yyyy-MM-dd-HH
  public static String formatDateTime(Long time) {
    LocalDateTime dateTimeToQuery = LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC);
    // Reformat the date to mirror the format of documents in Firestore: yyyy-MM-dd-HH.
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH", Locale.US).withZone(ZoneOffset.UTC);
    return formatter.format(dateTimeToQuery);
  }

  // Returns a list of DataShares for documents captured within the given query Cursor pair.
  private static List<DataShare> readDocumentsFromFirestore(
      FirestoreClient firestoreClient,
      String projectId,
      ImmutableTriple<Cursor, Cursor, StructuredQuery> cursors) {
    StructuredQuery.Builder queryBuilder = cursors.getRight().toBuilder();
    if (cursors.getLeft() != null) {
      queryBuilder.setStartAt(cursors.getLeft());
    }
    if (cursors.getMiddle() != null) {
      queryBuilder.setEndAt(cursors.getMiddle());
    }
    List<DataShare> docs = new ArrayList<>();
    try {
      ServerStream<RunQueryResponse> responseIterator =
          firestoreClient
              .runQueryCallable()
              .call(
                  RunQueryRequest.newBuilder()
                      .setStructuredQuery(queryBuilder.build())
                      .setParent(getParentPath(projectId))
                      .build());
      responseIterator.forEach(
          res -> {
            skippedResults.inc(res.getSkippedResults());
            // Streaming grpc may return partial results
            if (res.hasDocument()) {
              LOG.debug("Fetched document from Firestore: " + res.getDocument().getName());
              documentsRead.inc();
              try {
                docs.add(DataShare.from(res.getDocument()));
              } catch (InvalidDataShareException e) {
                LOG.warn("Invalid data share", e);
                invalidDocumentCounter.inc();
              }
            } else {
              partialProgress.inc();
            }
          });
    } catch (StatusRuntimeException e) {
      LOG.warn("grpc status exception", e);
      grpcException.inc();
    }
    return docs;
  }
}
