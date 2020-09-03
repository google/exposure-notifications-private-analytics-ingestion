/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.cloud.FirestoreClient;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline to export Exposure Notification Private Analytics data shares from Firestore and
 * translate into format usable by downstream batch processing by Health Authorities and
 * Facilitators.*
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 *
 * <pre>{@code
 * --project=YOUR_PROJECT_ID
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 */
public class IngestionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

  /**
   * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
   * not a letter.
   *
   * <p>It is used for tokenizing strings in the wordcount examples.
   */
  private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

  /**
   * Specific options for the pipeline.
   */
  public interface IngestionPipelineOptions extends PipelineOptions {
    /**
     * Path to the service account key json file.
     */
    @Description("Path to the service account key json file")
    @Required
    ValueProvider<String> getServiceAccountKey();

    void setServiceAccountKey(ValueProvider<String> value);

    /**
     * Fire base project to read from.
     */
    @Description("Firebase Project Id")
    @Default.String("appa-firebase-test")
    ValueProvider<String> getFirebaseProjectId();

    void setFirebaseProjectId(ValueProvider<String> value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Prefix of the output files to write to")
    @Default.String("gs://appa-batch-output/test-counts")
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);

    @Description(
        "Regex filter pattern to use in IngestionPipeline. "
            + "Only words matching this pattern will be counted.")
    @Default.String("test|metric")
    ValueProvider<String> getFilterPattern();

    void setFilterPattern(ValueProvider<String> value);
  }

  /**
   * This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
   * pipeline.
   */
  static class ExtractWordsFn extends DoFn<String, String> {

    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      // Split the line into words.
      String[] words = element.split(TOKENIZER_PATTERN, -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  /**
   * A SimpleFunction that converts a Word and Count into a printable string.
   */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {

    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   */
  public static class CountWords
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }

  // TODO(guray): convert this into a platform key attestation verifier?

  /**
   * A DoFn that filters for a specific key based upon a regular expression.
   */
  public static class FilterTextFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterTextFn.class);

    private final ValueProvider<String> pattern;

    private Pattern filter;

    public FilterTextFn(ValueProvider<String> pattern) {
      this.pattern = pattern;
    }

    private final Counter matchedWords = Metrics.counter(FilterTextFn.class, "matchedWords");

    private final Counter unmatchedWords = Metrics.counter(FilterTextFn.class, "unmatchedWords");

    @ProcessElement
    public void processElement(ProcessContext c) {
      // lazy init compiled pattern at runtime to pick up value provider
      if (filter == null) {
        filter = Pattern.compile(pattern.get());
      }
      if (filter.matcher(c.element().getKey()).matches()) {
        // Log at the "DEBUG" level each element that we match. When executing this pipeline
        // these log lines will appear only if the log level is set to "DEBUG" or lower.
        LOG.debug("Matched: " + c.element().getKey());
        matchedWords.inc();
        c.output(c.element());
      } else {
        // Log at the "TRACE" level each element that is not matched. Different log levels
        // can be used to control the verbosity of logging providing an effective mechanism
        // to filter less important information.
        LOG.trace("Did not match: " + c.element().getKey());
        unmatchedWords.inc();
      }
    }
  }

  static void runIngestionPipeline(IngestionPipelineOptions options) throws Exception {
    Pipeline p = Pipeline.create(options);

    // TODO(larryjacobs): Read documents from Firestore directly into a PCollection once such an I/O transform.
    Firestore db = initializeFirestore(options);
    List<String> docIds = readDocumentsFromFirestore(db, "metrics");
    p.apply(Create.of(docIds)).setCoder(StringUtf8Coder.of())
        .apply(new CountWords())
        .apply(ParDo.of(new FilterTextFn(options.getFilterPattern())))
        // TODO(guray): bail if not enough data shares to ensure min-k anonymity:
        // https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/Count.html#globally--
        .apply(MapElements.via(new FormatAsTextFn()))
        // TODO(justinowusu): s/TextIO/AvroIO/
        // https://beam.apache.org/releases/javadoc/2.4.0/org/apache/beam/sdk/io/AvroIO.html
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }

  // Initializes and returns a Firestore instance.
  static Firestore initializeFirestore(IngestionPipelineOptions pipelineOptions) throws Exception {
    // Don't attempt to initialize an already-initialized app. So far, this has only been an issue
    // when running unit tests.
    // TODO(larryjacobs): Figure out why this is happening in tests.
    if(FirebaseApp.getApps().isEmpty()) {
      // Use a service account to access Firestore.
      InputStream serviceAccount = new FileInputStream(pipelineOptions.getServiceAccountKey().get());
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
  static List<String> readDocumentsFromFirestore(Firestore db, String collection) throws Exception {
    // Create a reference to all collections and subcollections with the given collection id
    Query query = db.collectionGroup(collection);
    // Retrieve  query results asynchronously using query.get()
    ApiFuture<QuerySnapshot> querySnapshot = query.get();
    List<String> docs =  new ArrayList<>();

    for (DocumentSnapshot document : querySnapshot.get().getDocuments()) {
      LOG.debug("Fetched document from Firestore: " + document.getId());
      docs.add(document.getId());
    }

    return docs;
  }

  // Initializes and returns a Firestore instance.
  static Firestore initializeFirestore(IngestionPipelineOptions pipelineOptions) throws Exception {
    // Don't attempt to initialize an already-initialized app. So far, this has only been an issue
    // when running unit tests.
    if(FirebaseApp.getApps().isEmpty()) {
      // Use a service account to access Firestore.
      InputStream serviceAccount = new FileInputStream(pipelineOptions.getServiceAccountKey().get());
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
  static List<String> readDocumentsFromFirestore(Firestore db, String collection) throws Exception {
    // Create a reference to all collections and subcollections with the given collection id
    Query query = db.collectionGroup(collection);
    // Retrieve  query results asynchronously using query.get()
    ApiFuture<QuerySnapshot> querySnapshot = query.get();
    List<String> docs =  new ArrayList<>();

    for (DocumentSnapshot document : querySnapshot.get().getDocuments()) {
      LOG.debug("Fetched document from Firestore: " + document.getId());
      docs.add(document.getId());
    }

    return docs;
  }

  // Initializes and returns a Firestore instance.
  static Firestore initializeFirestore(IngestionPipelineOptions pipelineOptions) throws Exception {
    // Don't attempt to initialize an already-initialized app. So far, this has only been an issue
    // when running unit tests.
    if(FirebaseApp.getApps().isEmpty()) {
      // Use a service account to access Firestore.
      InputStream serviceAccount = new FileInputStream(pipelineOptions.getServiceAccountKey().get());
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
  static List<String> readDocumentsFromFirestore(Firestore db, String collection) throws Exception {
    // Create a reference to all collections and subcollections with the given collection id
    Query query = db.collectionGroup(collection);
    // Retrieve  query results asynchronously using query.get()
    ApiFuture<QuerySnapshot> querySnapshot = query.get();
    List<String> docs =  new ArrayList<>();

    for (DocumentSnapshot document : querySnapshot.get().getDocuments()) {
      LOG.debug("Fetched document from Firestore: " + document.getId());
      docs.add(document.getId());
    }

    return docs;
  }

  public static void main(String[] args) {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

    try {
      runIngestionPipeline(options);
    } catch (Exception e) {
      if (e instanceof UnsupportedOperationException) {
        // Apparently a known issue that this throws when generating a template:
        // https://issues.apache.org/jira/browse/BEAM-
      } else {
        LOG.debug("Exception thrown during pipeline run: " + e.toString());
      }
    }
  }
}
