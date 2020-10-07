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


import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import java.util.ArrayList;
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

    @Override
    public PCollection<DataShare> expand(PBegin input) {
      return input
          // TODO(larryjacobs): eliminate hack to kick off a DoFn where input doesn't matter (PBegin was giving errors)
          .apply(Create.of(""))
          // TODO(larryjacobs): run partition query to split into cursors, and pass that to ReadFn
          //      apparently the Source idiom is no longer favored?
          //        https://beam.apache.org/documentation/io/developing-io-overview/#sources
          //        https://beam.apache.org/blog/splittable-do-fn/
          // TODO(larryjacobs): reshuffle
          .apply(ParDo.of(new ReadFn()));
    }

    // TODO(larryjacobs): switch to take partitioned query cursors as input
    static class ReadFn extends DoFn<String, DataShare> {

      private Firestore db;

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        db = initializeFirestore(context.getPipelineOptions().as(IngestionPipelineOptions.class));
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        for (DataShare ds : readDocumentsFromFirestore(db, METRIC_COLLECTION_NAME)) {
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

  // Returns all document id's in the collections and subcollections with the given collection id.
  private static List<DataShare> readDocumentsFromFirestore(Firestore db, String collection)
      throws Exception {
    // Create a reference to all collections and subcollections with the given collection id
    Query query = db.collectionGroup(collection);
    // Retrieve query results asynchronously using query.get()
    // TODO(larryjacobs): scalable io connector to Firestore
    ApiFuture<QuerySnapshot> querySnapshot = query.get();
    List<DataShare> docs = new ArrayList<>();
    for (DocumentSnapshot document : querySnapshot.get().getDocuments()) {
      LOG.debug("Fetched document from Firestore: " + document.getReference().getPath());
      documentsRead.inc();
      try {
        docs.add(DataShare.from(document));
      } catch (RuntimeException e) {
        LOG.warn("Unable to read document " + document.getReference().getPath(), e);
        invalidDocumentCounter.inc();
      }
    }
    return docs;
  }
}
