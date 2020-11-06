package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.common.io.BaseEncoding;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulation of the specific manifest for a PHA or Facilitator data processor. Will lazily load
 * and parse the manifest json when a value is requested.
 *
 * <p>See
 * https://docs.google.com/document/d/1MdfM3QT63ISU70l63bwzTrxr93Z7Tv7EDjLfammzo6Q/edit#bookmark=id.8skgn5yx33ae
 */
public class DataProcessorManifest {

  private static final BaseEncoding BASE64 = BaseEncoding.base64();

  private final String manifestUrl;

  private String bucket;
  private Map<String, String> encryptionKeyIdMap;

  private boolean initialized = false;

  public DataProcessorManifest(String manifestUrl) {
    encryptionKeyIdMap = new HashMap<>();
    this.manifestUrl = manifestUrl;
  }

  public String getIngestionBucket() {
    init();
    return bucket;
  }

  public String mapEncryptionKeyId(String encryptionKeyId) {
    init();
    return encryptionKeyIdMap.get(encryptionKeyId);
  }

  private synchronized void init() {
    if (initialized) {
      return;
    }
    try {
      JsonObject manifestJson = fetchAndParseJson();
      bucket = manifestJson.get("ingestion-bucket").getAsString();
      populateMap(manifestJson.getAsJsonObject("packet-encryption-certificates"));
      initialized = true;
    } catch (IOException e) {
      throw new RuntimeException("Unable to fetch and parse manifest", e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to hash and encode certificate", e);
    }
  }

  private JsonObject fetchAndParseJson() throws IOException {
    URL url = new URL(manifestUrl);
    InputStreamReader manifestReader = new InputStreamReader(url.openStream());
    return new JsonParser().parse(manifestReader).getAsJsonObject();
  }

  private void populateMap(JsonObject encryptionCerts) throws NoSuchAlgorithmException {
    for (Map.Entry<String, JsonElement> entry : encryptionCerts.entrySet()) {
      String encryptionKeyId = entry.getKey();
      String certificate = entry.getValue().getAsJsonObject().get("certificate").getAsString();
      encryptionKeyIdMap.put(hashAndEncode(certificate), encryptionKeyId);
    }
  }

  public static String hashAndEncode(String... content) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (String s : content) {
      digest.update(s.getBytes());
    }
    return BASE64.encode(digest.digest());
  }
}
