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

import com.amazonaws.auth.AWSCredentialsProvider;
import java.time.Clock;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Specific options for the pipeline. */
public interface IngestionPipelineOptions extends DataflowPipelineOptions {

  int UNSPECIFIED_START = -1;

  /** Firestore Project */
  @Description("Firestore Project")
  @Default.String("")
  String getFirestoreProject();

  void setFirestoreProject(String value);

  /** PHA Manifest file URL. */
  @Description("PHA Manifest file URL")
  @Default.String("")
  String getPhaManifestURL();

  void setPhaManifestURL(String value);

  /** PHA AWS bucket region. */
  @Description("PHA AWS bucket region")
  @Default.String("")
  String getPhaAwsBucketRegion();

  void setPhaAwsBucketRegion(String value);

  /** PHA AWS bucket name. */
  @Description("PHA AWS bucket name")
  @Default.String("")
  String getPhaAwsBucketName();

  void setPhaAwsBucketName(String value);

  /** PHA AWS bucket role. */
  @Description("PHA AWS bucket role")
  @Default.String("")
  String getPhaAwsBucketRole();

  void setPhaAwsBucketRole(String value);

  /**
   * Directory to place output files for PHA. If the directory does not exist, then it will
   * automatically be created.
   *
   * <p>If set, this flag overrides an output location set in the PHA manifest file.
   */
  @Description(
      "Directory to place output files for PHA (Should end in 2-letter state abbreviation).")
  @Default.String("")
  String getPhaOutput();

  void setPhaOutput(String value);

  /** Facilitator Manifest file URL. */
  @Description("Facilitator Manifest file URL")
  @Default.String("")
  String getFacilitatorManifestURL();

  void setFacilitatorManifestURL(String value);

  /** Facilitator AWS bucket region. */
  @Description("Facilitator AWS bucket region")
  @Default.String("")
  String getFacilitatorAwsBucketRegion();

  void setFacilitatorAwsBucketRegion(String value);

  /** Facilitator AWS bucket name. */
  @Description("Facilitator AWS bucket name")
  @Default.String("")
  String getFacilitatorAwsBucketName();

  void setFacilitatorAwsBucketName(String value);

  /** Facilitator AWS bucket role. */
  @Description("Facilitator AWS bucket role")
  @Default.String("")
  String getFacilitatorAwsBucketRole();

  void setFacilitatorAwsBucketRole(String value);

  /**
   * Directory to place output files for Facilitator. If the directory does not exist, then it will
   * automatically be created.
   *
   * <p>If set, this flag overrides an output location set in the Facilitator manifest file.
   */
  @Description(
      "Directory to place output files for Facilitator. (Should end in 2-letter state"
          + " abbreviation)")
  @Default.String("")
  String getFacilitatorOutput();

  void setFacilitatorOutput(String value);

  /**
   * Start time of window to process. Used to filter documents that have been read from Firestore on
   * the "Creation" field. Defaults to current time rounded down to previous alignment period based
   * on the duration.
   */
  @Description("Start time in UTC seconds of documents to process")
  @Default.Long(UNSPECIFIED_START)
  Long getStartTime();

  void setStartTime(Long value);

  /**
   * Duration of time window to process. Used to filter documents that have been read from Firestore
   * on the "Creation" field.
   */
  @Description("Duration of window in seconds")
  @Default.Long(3600)
  Long getDuration();

  void setDuration(ValueProvider<Long> value);


  /**
   * Hours to look before startTime when querying Firestore collection. Used to construct document
   * path for Firestore reads.
   */
  @Description(
      "Hours to read backwards from startTime. Used to construct document path for Firestore"
          + " reads.")
  @Default.Long(1)
  Long getGraceHoursBackwards();

  void setGraceHoursBackwards(Long value);

  /**
   * Hours to look before startTime when querying Firestore. Used to construct document path for
   * Firestore reads.
   */
  @Description(
      "Hours to read forward from startTime. Used to construct document path for Firestore"
          + " reads.")
  @Default.Long(1)
  Long getGraceHoursForwards();

  void setGraceHoursForwards(Long value);

  /** Maximum number of query partitions to create for running Firestore read. */
  @Description("Maximum number of partitions to create for Firestore query.")
  @Default.Long(20)
  Long getPartitionCount();

  void setPartitionCount(Long value);

  /** Batch size of individual files. */
  @Description("Batch size of individual files.")
  @Default.Long(200000)
  Long getBatchSize();

  void setBatchSize(Long value);

  /** Batch size of Firestore batch deletes. */
  @Description("Batch size of Firestore deletes.")
  @Default.Long(100)
  Long getDeleteBatchSize();

  void setDeleteBatchSize(Long value);

  /**
   * Signing key resource name. See https://cloud.google.com/kms/docs/resource-hierarchy E.g.,
   * projects/$PROJECT_NAME/locations/global/keyRings/$RING/cryptoKeys/$KEY/cryptoKeyVersions/$VERSION
   */
  @Description("KMS resource name for signature generation")
  @Default.String("")
  String getKeyResourceName();

  void setKeyResourceName(String value);

  /** Whether to check device hardware attestations */
  @Description("Verify device attestations")
  @Default.Boolean(true)
  Boolean getDeviceAttestation();

  void setDeviceAttestation(Boolean value);

  /** Package name to be verified in the key attestation certificate. */
  @Description("Package name of the Android application.")
  @Default.String("")
  String getPackageName();

  void setPackageName(String value);

  /**
   * Package digest (HEX(SHA256)) of the Android application, to be verified in the key attestation
   * certificate.
   */
  @Description("Package signature digest of the Android application.")
  @Default.String("")
  String getPackageSignatureDigest();

  void setPackageSignatureDigest(String value);

  @Description("AWS region used by the AWS client")
  String getAwsRegion();

  void setAwsRegion(String value);

  @Description(
      "The credential instance that should be used to authenticate against AWS services. The option value must contain \"@type\" field and an AWS Credentials Provider class name as the field value. Refer to DefaultAWSCredentialsProviderChain Javadoc for usage help. For example, to specify the AWS key ID and secret, specify the following: {\"@type\": \"AWSStaticCredentialsProvider\", \"awsAccessKeyId\":\"<key_id>\", \"awsSecretKey\":\"<secret_key>\"}")
  @Default.InstanceFactory(AwsOptions.AwsUserCredentialsFactory.class)
  AWSCredentialsProvider getAwsCredentialsProvider();

  void setAwsCredentialsProvider(AWSCredentialsProvider value);

  /**
   * @return {@code startTime} from options/flags if set. Otherwise, rounds current time down to
   *     start of {@code numWindows} windows back of length {@code duration} option/flag.
   */
  static long calculatePipelineStart(long start, long duration, int numWindows, Clock clock) {
    if (start != UNSPECIFIED_START) {
      return start;
    }
    return ((clock.instant().getEpochSecond() / duration) - numWindows) * duration;
  }
}
