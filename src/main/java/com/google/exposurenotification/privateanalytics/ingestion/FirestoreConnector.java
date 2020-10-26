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
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
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

  private static final Counter skippedResults =
      Metrics.counter(FirestoreConnector.class, "skippedResults");

  private static final Counter partialProgress =
      Metrics.counter(FirestoreConnector.class, "partialProgress");

  /** Reads documents from Firestore */
  public static final class FirestoreReader extends PTransform<PBegin, PCollection<DataShare>> {

    @Override
    public PCollection<DataShare> expand(PBegin input) {
      return input
          // TODO(larryjacobs): eliminate hack to kick off a DoFn where input doesn't matter (PBegin
          // was giving errors)
          .apply("Begin", Create.of(""))
          .apply("GenerateQueries", ParDo.of(new GenerateQueriesFn()))
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

    /**
     * Generate a query for each hour within the given time window, specified by the Duration
     * pipeline option.
     */
    static class GenerateQueriesFn extends DoFn<String, StructuredQuery> {

      private static final long SECONDS_IN_HOUR = Duration.of(1, ChronoUnit.HOURS).getSeconds();
      // Order must be name ascending. Right now, this is the only ordering that the
      // Firestore SDK supports.
      private static final String NAME_FIELD = "__name__";

      @ProcessElement
      public void processElement(ProcessContext context) {
        IngestionPipelineOptions options =
            context.getPipelineOptions().as(IngestionPipelineOptions.class);
        long startTime = options.getStartTime();
        long backwardWindow = options.getGracePeriodBackwards() / SECONDS_IN_HOUR;
        long forwardWindow =
            (options.getDuration() + options.getGracePeriodForwards()) / SECONDS_IN_HOUR;

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
          context.output(query);
          queriesGenerated.inc();
        }
      }
    }

    static class PartitionQueryFn
        extends DoFn<StructuredQuery, ImmutableTriple<Cursor, Cursor, StructuredQuery>> {

      private FirestoreClient client;

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
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
      public void startBundle(StartBundleContext context) throws Exception {
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

  /** Deletes documents from Firestore */
  public static final class FirestoreDeleter extends PTransform<PCollection<DataShare>, PDone> {

    @Override
    public PDone expand(PCollection<DataShare> input) {
      // TODO: would it be useful to sort on document paths to get more efficient deletes?
      input.apply("Delete", ParDo.of(new DeleteFn()));
      return PDone.in(input.getPipeline());
    }

    // TODO: batch up deletes
    // https://firebase.google.com/docs/firestore/manage-data/delete-data#collections
    // https://github.com/googleapis/nodejs-firestore/issues/64
    static class DeleteFn extends DoFn<DataShare, Void> {

      private FirestoreClient client;

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
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
        if (options.getDelete()
            && context.element() != null
            && context.element().getPath() != null) {
          client.deleteDocument(context.element().getPath());
          documentsDeleted.inc();
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

  private static void shutdownFirestoreClient(FirestoreClient client) {
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
