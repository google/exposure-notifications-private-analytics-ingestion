package com.google.exposurenotification.privateanalytics.ingestion;

import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSFederatedAuthHelper {
  private static final Logger LOG = LoggerFactory.getLogger(AWSFederatedAuthHelper.class);

  public static void setupAWSAuth(IngestionPipelineOptions options, String role, String region)
      throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (!(credentials instanceof IdTokenProvider)) {
      throw new IllegalArgumentException("Credentials are not an instance of IdTokenProvider.");
    }

    AWSSecurityTokenService stsClient =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .withRegion(region)
            .build();

    // Obtain credentials for the IAM role. Note that you cannot assume the role of an AWS root
    // account;
    // Amazon S3 will deny access. You must use credentials for an IAM user or an IAM role.
    AssumeRoleWithWebIdentityRequest roleRequest =
        new AssumeRoleWithWebIdentityRequest()
            .withRoleArn(role)
            .withRoleSessionName("enpa-gcp-aws-session")
            .withWebIdentityToken(
                ((IdTokenProvider) credentials)
                    .idTokenWithAudience("enpa-gcp-aws", null)
                    .getTokenValue());

    AssumeRoleWithWebIdentityResult roleResponse = stsClient.assumeRoleWithWebIdentity(roleRequest);
    Credentials sessionCredentials = roleResponse.getCredentials();

    // Create a BasicSessionCredentials object that contains the credentials you just retrieved.
    BasicSessionCredentials awsCredentials =
        new BasicSessionCredentials(
            sessionCredentials.getAccessKeyId(),
            sessionCredentials.getSecretAccessKey(),
            sessionCredentials.getSessionToken());

    options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));
    options.setAwsRegion(region);
  }
}
