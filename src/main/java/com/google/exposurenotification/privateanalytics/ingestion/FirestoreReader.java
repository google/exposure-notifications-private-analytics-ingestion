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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirestoreReader extends PTransform<PBegin, PCollection<DataShare>> {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreReader.class);

  @Override
  public PCollection<DataShare> expand(PBegin input) {
    try {
      Firestore db = initializeFirestore((IngestionPipelineOptions) input.getPipeline().getOptions());
      return readDocumentsFromFirestore(db, "metrics", input.getPipeline());
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize Firestore connection", e);
    }
  }

  // Initializes and returns a Firestore instance.
  private Firestore initializeFirestore(IngestionPipelineOptions pipelineOptions) throws Exception {
    if (FirebaseApp.getApps().isEmpty()) {
      InputStream serviceAccount = new FileInputStream(
          pipelineOptions.getServiceAccountKey().get());
      GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
      FirebaseOptions options = new FirebaseOptions.Builder()
          .setProjectId(pipelineOptions.getFirebaseProjectId().get())
          .setCredentials(credentials)
          .build();
      FirebaseApp.initializeApp(options);
    }

    return FirestoreClient.getFirestore();
  }

  // Returns all document id's in the collections and subcollections with the given collection id.
  private PCollection<DataShare> readDocumentsFromFirestore(Firestore db, String collection,
      Pipeline p) throws Exception {
    // Create a reference to all collections and subcollections with the given collection id
    Query query = db.collectionGroup(collection);
    // Retrieve query results asynchronously using query.get()
    // TODO(larryjacobs): scalable io connector to Firestore
    ApiFuture<QuerySnapshot> querySnapshot = query.get();
    List<DataShare> docs = new ArrayList<>();
    for (DocumentSnapshot document : querySnapshot.get().getDocuments()) {
      LOG.debug("Fetched document from Firestore: " + document.getId());
      docs.add(document.toObject(DataShare.class));
    }
    return p.apply(Create.of(docs));
  }
}
