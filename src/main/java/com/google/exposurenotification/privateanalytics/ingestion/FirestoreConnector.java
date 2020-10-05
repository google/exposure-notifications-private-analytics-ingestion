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
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Primitive beam connector for Firestore native specific to ENPA.
 *
 * For a general purpose connector see https://issues.apache.org/jira/browse/BEAM-8376
 */
public class FirestoreConnector {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreConnector.class);

  private static final String METRIC_COLLECTION_NAME = "metrics";

  private static final Counter invalidDocumentCounter = Metrics
      .counter(FirestoreConnector.class, "invalidDocuments");

  private static final Counter documentsRead = Metrics
      .counter(FirestoreConnector.class, "documentsRead");

  /** Reads documents from Firestore */
  public static final class FirestoreReader extends PTransform<PBegin, PCollection<DataShare>> {
    private static final Logger LOG = LoggerFactory.getLogger(
        FirestoreReader.class);
    // Order must be name ascending. Right now, this is the only ordering that the
    // Firestore SDK supports.
    private final String NAME_FIELD = "__name__";

    @Override
    public PCollection<DataShare> expand(PBegin input) {
      StructuredQuery query = StructuredQuery.newBuilder()
          .addFrom(
              CollectionSelector.newBuilder()
                  .setCollectionId(METRIC_COLLECTION_NAME)
                  .setAllDescendants(true)
                  .build())
          .addOrderBy(
              Order.newBuilder()
                  .setField(FieldReference.newBuilder()
                      .setFieldPath(NAME_FIELD)
                      .build())
                  .setDirection(Direction.ASCENDING)
                  .build())
          .build();

      return input
          // TODO(larryjacobs): eliminate hack to kick off a DoFn where input doesn't matter (PBegin was giving errors)
          .apply(Create.of(""))
          .apply(ParDo.of(new PartitionQueryFn(query)))
          // TODO(larryjacobs): reshuffle if necessary at scale
          .apply(ParDo.of(new ReadFn(query)));
    }

    static class PartitionQueryFn extends DoFn<String, ImmutablePair<Cursor, Cursor>> {
      private StructuredQuery query;
      private com.google.cloud.firestore.v1.FirestoreClient client;

      public PartitionQueryFn(StructuredQuery query) {
        this.query = query;
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        client = getFirestoreClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        IngestionPipelineOptions options = context.getPipelineOptions().as(IngestionPipelineOptions.class);
        PartitionQueryRequest request =
            PartitionQueryRequest.newBuilder()
                .setPartitionCount(options.getPartitionCount().get())
                .setParent(
                    getParentPath(options))
                .setStructuredQuery(query)
                .build();
        PartitionQueryPagedResponse response =
            client.partitionQuery(request);
        Iterator<Cursor> iterator = response.iterateAll().iterator();

        // Return a Cursor pair to represent the start and end points within which to run the query.
        Cursor start = null;
        Cursor end;
        while (iterator.hasNext()) {
          end = iterator.next();
          context.output(ImmutablePair.of(start, end));
          start = end;
        }
        context.output(ImmutablePair.of(start, null));
      }
    }

    static class ReadFn extends DoFn<ImmutablePair<Cursor, Cursor>, DataShare> {

      private com.google.cloud.firestore.v1.FirestoreClient client;
      private StructuredQuery query;

      public ReadFn(StructuredQuery query) {
        this.query = query;
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        client = getFirestoreClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        for (DataShare ds : readDocumentsFromFirestore(client,
            context.getPipelineOptions().as(IngestionPipelineOptions.class), query,
            context.element())) {
          context.output(ds);
        }
      }
    }
  }

  /** Deletes documents from Firestore */
  public static final class FirestoreDeleter extends PTransform<PCollection<DataShare>, PDone> {

    @Override
    public PDone expand(PCollection<DataShare> input) {
      // TODO: would it be useful to sort on document paths to get more efficient deletes?
      input.apply(ParDo.of(new DeleteFn()));
      return PDone.in(input.getPipeline());
    }

    // TODO: batch up deletes
    // https://firebase.google.com/docs/firestore/manage-data/delete-data#collections
    // https://github.com/googleapis/nodejs-firestore/issues/64
    static class DeleteFn extends DoFn<DataShare, Void> {

      private Firestore db;

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        db = initializeFirestore(context.getPipelineOptions().as(IngestionPipelineOptions.class));
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        IngestionPipelineOptions options = context.getPipelineOptions().as(IngestionPipelineOptions.class);
        // TODO: way to short circuit this earlier based on a ValueProvider flag?
        if (options.getDelete().get() && context.element() != null
            && context.element().getPath() != null) {
          db.document(context.element().getPath()).delete();
        }
      }
    }
  }


  // Initializes and returns a Firestore instance.
  private static Firestore initializeFirestore(IngestionPipelineOptions pipelineOptions)
      throws Exception {
    if (FirebaseApp.getApps().isEmpty()) {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      FirebaseOptions options = new FirebaseOptions.Builder()
          .setProjectId(pipelineOptions.getFirebaseProjectId().get())
          .setCredentials(credentials)
          .build();
      FirebaseApp.initializeApp(options);
    }

    return FirestoreClient.getFirestore();
  }

  private static com.google.cloud.firestore.v1.FirestoreClient getFirestoreClient()
      throws IOException {
    FirestoreSettings settings =
        FirestoreSettings.newBuilder().setCredentialsProvider(
            FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault())).build();
    return com.google.cloud.firestore.v1.FirestoreClient.create(settings);
  }

  private static String getParentPath(IngestionPipelineOptions options) {
    return "projects/" + options.getFirebaseProjectId() + "/databases/(default)/documents";
  }

  // Returns a list of DataShares for documents captured within the given query Cursor pair.
  private static List<DataShare> readDocumentsFromFirestore(
      com.google.cloud.firestore.v1.FirestoreClient firestoreClient, IngestionPipelineOptions options, StructuredQuery query,
      ImmutablePair<Cursor, Cursor> cursors) {
    StructuredQuery.Builder queryBuilder = query.toBuilder();
    if (cursors.getLeft() != null)  {
      queryBuilder.setStartAt(cursors.getLeft());
    }
    if (cursors.getRight() != null) {
      queryBuilder.setEndAt(cursors.getRight());
    }
    ServerStream<RunQueryResponse> responseIterator =
        firestoreClient.runQueryCallable()
            .call(RunQueryRequest.newBuilder()
                .setStructuredQuery(queryBuilder.build())
                .setParent(getParentPath(options))
                .build());
    List<DataShare> docs = new ArrayList<>();
    // TODO(larryjacobs): rather than retrieve all documents (and filter for time later) retrieve only the date subcollection corresponding to the window of interest +/- an hour
    responseIterator.forEach(res -> {
      LOG.debug("Fetched document from Firestore: " + res.getDocument().getName());
      documentsRead.inc();
      try {
        docs.add(DataShare.from(res.getDocument()));
      } catch (RuntimeException e) {
        LOG.warn("Unable to read document " + res.getDocument().getName(), e);
        invalidDocumentCounter.inc();
      }
    });
    return docs;
  }
}
