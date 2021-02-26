package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.auto.service.AutoService;
import com.google.exposurenotification.privateanalytics.ingestion.attestation.AbstractDeviceAttestation;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

@AutoService(AbstractDeviceAttestation.class)
public class TestAttestation extends AbstractDeviceAttestation {

  public static final String UUID = "13d94c5e-a69c-427b-8cde-79863601c251";
  public static final long CREATED_TIME = 1604039801L;

  @Override
  public PCollection<DataShare> expand(PCollection<DataShare> input) {
    return input.apply(
        Filter.by(
            new SerializableFunction<DataShare, Boolean>() {
              @Override
              public Boolean apply(DataShare input) {
                return "invalidSignature".equals(input.getSignature());
              }
            }));
  }

  @Override
  public Class<? extends PipelineOptions> getOptionsClass() {
    return null;
  }

  public static Map<String, Value> getValidDocFields() {
    Map<String, Value> fields = new HashMap<>();

    // Certificate chain.
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setStringValue(
                                "MIICyDCCAm2gAwIBAgIBATAMBggqhkjOPQQDAgUAMC8xGTAXBgNVBAUTEDkwZThkYTNjYWRmYzc4MjAxEjAQBgNVBAwMCVN0cm9uZ0JveDAeFw0wMTA1MTQxMTE5MTNaFw0wMTA3MTUyMzE4MDBaMB8xHTAbBgNVBAMMFEFuZHJvaWQgS2V5c3RvcmUgS2V5MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAENC2FzdRbP8wuDriXCDGyBewgdO8tEbnZ68jPWuqqmIajzzzCoZJmIxTxaRhEALPXrX1VrUZmQ6fZ81WAtjf2SKOCAYYwggGCMA4GA1UdDwEB/wQEAwIHgDCCAW4GCisGAQQB1nkCAREEggFeMIIBWgIBBAoBAgIBKQoBAgQgq2wFkiu71rf0UeaRjSyFdas0TvptGzoMuhZ284j4qaUEADCBhb+DEQgCBgF1ex6LKL+DEggCBgF1ex6LKL+FPQgCBgF1eswlsb+FRV0EWzBZMTMwMQQsY29tLmdvb2dsZS5hbmRyb2lkLmFwcHMuZXhwb3N1cmVub3RpZmljYXRpb24CAQExIgQgN3X23FDoH2zp6mldxSrVqY4oBntFBDYM4HdzkJUFZH8wgZ+hCDEGAgECAgEDogMCAQOjBAICAQClBTEDAgEEv4N3AgUAv4U+AwIBAL+FQEwwSgQgrmMWtHU8YfWFW5W5uYSEr3hPLoNkjQ/MgQf8p1LK6jQBAf8KAQAEIPtw3qC/EBlRVpcaKdG0jq/+YGVboTMGTTbpzsQm+FMev4VBBQIDAa2wv4VCBQIDAxUbv4VOBgIEATQ+kb+FTwYCBAE0PpEwDAYIKoZIzj0EAwIFAANHADBEAiAl4DVLeXUzEXGJMsL/NvTatwMuonyQjUbVA8LNeUC+5gIgQ2kqCm/vjMPya4+PdShF2oUGJRcdLcj2DexhRJvFCMU=")
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setStringValue(
                                "MIICMDCCAbegAwIBAgIKESM4JDRACGgBcTAKBggqhkjOPQQDAjAvMRkwFwYDVQQFExBjY2QxOGI5YjYwOGQ2NThlMRIwEAYDVQQMDAlTdHJvbmdCb3gwHhcNMTgwNTI1MjMyODUwWhcNMjgwNTIyMjMyODUwWjAvMRkwFwYDVQQFExA5MGU4ZGEzY2FkZmM3ODIwMRIwEAYDVQQMDAlTdHJvbmdCb3gwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATkV0TCsZ+vcIoXK0BLe4q4sQ1veBPE228LqldQCQPCb6IBCpM7rHDgKmsaviWtsA0anJyUpXHTVix0mdIy9Xcno4G6MIG3MB0GA1UdDgQWBBRvsbUxnba4hRW+z8AMdxqP51TqljAfBgNVHSMEGDAWgBS8W8vVecaU3BmPm59nU8zr5mLf3jAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwICBDBUBgNVHR8ETTBLMEmgR6BFhkNodHRwczovL2FuZHJvaWQuZ29vZ2xlYXBpcy5jb20vYXR0ZXN0YXRpb24vY3JsLzExMjMzODI0MzQ0MDA4NjgwMTcxMAoGCCqGSM49BAMCA2cAMGQCMFBzxlbrGJarX+e8d7UfD5M2Br3QxKUFAS1tfGxy9Lw72yfFn8v3jxNyCamglqpw8gIwYkzbZDvx/uU6vXIaB1y0PRGq5Jp5xIgKqUEJvsBuyMN8JdJsfzvHbkYyZUujU/SV")
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setStringValue(
                                "MIID1zCCAb+gAwIBAgIKA4gmZ2BliZaFmDANBgkqhkiG9w0BAQsFADAbMRkwFwYDVQQFExBmOTIwMDllODUzYjZiMDQ1MB4XDTE4MDYyMDIyMTQwMloXDTI4MDYxNzIyMTQwMlowLzEZMBcGA1UEBRMQY2NkMThiOWI2MDhkNjU4ZTESMBAGA1UEDAwJU3Ryb25nQm94MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEhO8/KkOtlIwEItY5zds+E2p/WG0wNet8fDHsooCiZi0MMzJZMGTlepRhYbnCBQlSi7TXjTzQQ8kAJmJFeHTlp7hBcpccDbCyicyvX5JjazVOiB3hwKzS0oKwSS9D3sUfo4G2MIGzMB0GA1UdDgQWBBS8W8vVecaU3BmPm59nU8zr5mLf3jAfBgNVHSMEGDAWgBQ2YeEAfIgFCVGLRGxH/xpMyepPEjAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwICBDBQBgNVHR8ESTBHMEWgQ6BBhj9odHRwczovL2FuZHJvaWQuZ29vZ2xlYXBpcy5jb20vYXR0ZXN0YXRpb24vY3JsLzhGNjczNEM5RkE1MDQ3ODkwDQYJKoZIhvcNAQELBQADggIBAJOSNuBkQfic/SZf++OB6bXkbHmJpZaHxU/yVtnOZBrhAa4cLKIDg9A9A3nWtLw68x8VfI1s442+qHWfxGvVidhaCsLT+F2dpUme5VsgJSAK/6ZTLb5vhwskzS6G8YPUM/NzeFif7tkMu9cHkHlCFwJePPVWBg0iz51PFphdJGOG3e3CsRHG37Lk5RlvrVt3R5toRDrK5QV5V1RQ6OadRxHBxmmRC2owao8fU5yYkZ42bznwkyqCc0WsHmpqI0D/6jPaszAE7HlGPLMtGo/rVEaRjrjg9huEJMAHIsQAxhUDfZwAZ6tE4jEVf52o3AezZsvzDErcwWPB6ekUMBG9zuNLipcEhLKG9X4V0tJN+vwqvUWrzen9ZzvSoN6p5rQNjPFNvVtq0rVzPoPHjF6wN9r2qsQA8MVY31b3maOVq9n+WVOXxaZXtMmIKi8EgZAejeaq2ewAuxYaXoHsLI/9GPtF0k4mCbN6dffMwh/RJ8IWfZ3stwbyzcJ+sIrQ9IWX/Wsdi4vo3ZgRhf85pbGYM8SFYhnjUAbiyBHPYb0wltu+zwXMKXSuSCVhq3BnuTHS6sEkK5u9QBFY+U4AnP74MJ0tjP9S4YXm5/neTcE16yAdZlYY/8qZZoiukXl4TZTqlZA7/H5sdSx9ps+w/izJRS6CrFa72mB/tPtCd3jbMxVg")
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setStringValue(
                                "MIIFYDCCA0igAwIBAgIJAOj6GWMU0voYMA0GCSqGSIb3DQEBCwUAMBsxGTAXBgNVBAUTEGY5MjAwOWU4NTNiNmIwNDUwHhcNMTYwNTI2MTYyODUyWhcNMjYwNTI0MTYyODUyWjAbMRkwFwYDVQQFExBmOTIwMDllODUzYjZiMDQ1MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAr7bHgiuxpwHsK7Qui8xUFmOr75gvMsd/dTEDDJdSSxtf6An7xyqpRR90PL2abxM1dEqlXnf2tqw1Ne4Xwl5jlRfdnJLmN0pTy/4lj4/7tv0Sk3iiKkypnEUtR6WfMgH0QZfKHM1+di+y9TFRtv6y//0rb+T+W8a9nsNL/ggjnar86461qO0rOs2cXjp3kOG1FEJ5MVmFmBGtnrKpa73XpXyTqRxB/M0n1n/W9nGqC4FSYa04T6N5RIZGBN2z2MT5IKGbFlbC8UrW0DxW7AYImQQcHtGl/m00QLVWutHQoVJYnFPlXTcHYvASLu+RhhsbDmxMgJJ0mcDpvsC4PjvB+TxywElgS70vE0XmLD+OJtvsBslHZvPBKCOdT0MS+tgSOIfga+z1Z1g7+DVagf7quvmag8jfPioyKvxnK/EgsTUVi2ghzq8wm27ud/mIM7AY2qEORR8Go3TVB4HzWQgpZrt3i5MIlCaY504LzSRiigHCzAPlHws+W0rB5N+er5/2pJKnfBSDiCiFAVtCLOZ7gLiMm0jhO2B6tUXHI/+MRPjy02i59lINMRRev56GKtcd9qO/0kUJWdZTdA2XoS82ixPvZtXQpUpuL12ab+9EaDK8Z4RHJYYfCT3Q5vNAXaiWQ+8PTWm2QgBR/bkwSWc+NpUFgNPN9PvQi8WEg5UmAGMCAwEAAaOBpjCBozAdBgNVHQ4EFgQUNmHhAHyIBQlRi0RsR/8aTMnqTxIwHwYDVR0jBBgwFoAUNmHhAHyIBQlRi0RsR/8aTMnqTxIwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAYYwQAYDVR0fBDkwNzA1oDOgMYYvaHR0cHM6Ly9hbmRyb2lkLmdvb2dsZWFwaXMuY29tL2F0dGVzdGF0aW9uL2NybC8wDQYJKoZIhvcNAQELBQADggIBACDIw41L3KlXG0aMiS//cqrG+EShHUGo8HNsw30W1kJtjn6UBwRM6jnmiwfBPb8VA91chb2vssAtX2zbTvqBJ9+LBPGCdw/E53Rbf86qhxKaiAHOjpvAy5Y3m00mqC0w/Zwvju1twb4vhLaJ5NkUJYsUS7rmJKHHBnETLi8GFqiEsqTWpG/6ibYCv7rYDBJDcR9W62BW9jfIoBQcxUCUJouMPH25lLNcDc1ssqvC2v7iUgI9LeoM1sNovqPmQUiG9rHli1vXxzCyaMTjwftkJLkf6724DFhuKug2jITV0QkXvaJWF4nUaHOTNA4uJU9WDvZLI1j83A+/xnAJUucIv/zGJ1AMH2boHqF8CY16LpsYgBt6tKxxWH00XcyDCdW2KlBCeqbQPcsFmWyWugxdcekhYsAWyoSf818NUsZdBWBaR/OukXrNLfkQ79IyZohZbvabO/X+MVT3rriAoKc8oE2Uws6DF+60PV7/WIPjNvXySdqspImSN78mflxDqwLqRBYkA3I75qppLGG9rp7UCdRjxMl8ZDBld+7yvHVgt1cVzJx9xnyGCC23UaicMDSXYrB4I4WHXPGjxhZuCuPBLTdOLU8YRvMYdEvYebWHMpvwGCF6bAx3JBpIeOQ1wDB5y0USicV3YgYGmi+NZfhA4URSh77Yd6uuJOJENRaNVTzk")
                            .build())
                    .build())
            .build());
    // Signature.
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(
                "MEUCIQDHeJr8GlT4X1br+O5zrU4drai6tCQ7Z/6DF2jQoI5m2wIgCc+VyGAcNcd2aBIwQ3VrmWwM9xCugCTguLhlqUyOmts=")
            .build());
    // Prio params.
    Map<String, Value> prioParams = new HashMap<>();
    prioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(10).build());
    prioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(100.0).build());
    prioParams.put(
        DataShare.NUMBER_OF_SERVERS_FIELD, Value.newBuilder().setIntegerValue(2).build());
    prioParams.put(DataShare.PRIME_FIELD, Value.newBuilder().setIntegerValue(4293918721L).build());
    // Encrypted data shares.
    Map<String, Value> share1 = new HashMap<>();
    share1.put(
        DataShare.ENCRYPTION_KEY_ID,
        Value.newBuilder().setStringValue("O1ZVwG0jbYC61PsIN33yU+TMLbtfZd5Pywv5TUt2G2o=").build());
    share1.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setStringValue(
                "BGi3cl2yoxXJsJkzWUMgf8GdcodIlt+0EGrSAkbh5e0KWgepJh4zIxY9Rg9BsaxqH6Qt12hRgvvnwvAEkE14fy05Gh99swkIHuArf4qznU7BTTdXUks3cx9agPFjf3hG9s2UF0zB22puOwjTF4Nvo91nTS/qMltBPDJhL9FkpM4zyBi7QbwfRFGTnOZVXgOdPoguqpOqNb4wD9t1LBh+xE22sJVWKUN+sC1Y70e5iboFVM84jabTo8aySmZp1UAPSMaYeYM=")
            .build());
    Map<String, Value> share2 = new HashMap<>();
    share2.put(
        DataShare.ENCRYPTION_KEY_ID,
        Value.newBuilder().setStringValue("VwFUVJtSrkNZUszrbVvE7x85LVc5HnlnLv6EtuKH/os=").build());
    share2.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setStringValue(
                "BNWQJlfKoGMzsOSbwCgrg+go0v9GrHMKSIjZ/uLhCQMUTdDsa0VOWy7P1H9ptKRwxaT1UYHcJFc0vNIzf8QEujCh3fmaI4DU7yExsgnvLIv/Fl0clGclLy0UrfAnMIvSnQ17CcNzOt6MvjEwiwMwTQQ=")
            .build());
    List<Value> encryptedDataShares = new ArrayList<>();
    encryptedDataShares.add(
        Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(share1).build()).build());
    encryptedDataShares.add(
        Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(share2).build()).build());

    // Payload
    Map<String, Value> payload = new HashMap<>();
    payload.put(DataShare.UUID, Value.newBuilder().setStringValue(UUID).build());
    payload.put(DataShare.SCHEMA_VERSION, Value.newBuilder().setIntegerValue(1).build());
    payload.put(
        DataShare.PRIO_PARAMS,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(prioParams).build())
            .build());
    payload.put(
        DataShare.ENCRYPTED_DATA_SHARES,
        Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder().addAllValues(encryptedDataShares).build())
            .build());
    payload.put(
        DataShare.CREATED,
        Value.newBuilder()
            .setTimestampValue(Timestamp.newBuilder().setSeconds(CREATED_TIME).build())
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(payload).build())
            .build());
    return fields;
  }
}
