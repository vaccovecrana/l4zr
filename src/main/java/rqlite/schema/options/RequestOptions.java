package rqlite.schema.options;

import java.time.Duration;

public class RequestOptions {
  @UValue(value = "transaction", omitEmpty = true)
  public boolean transaction;
  @UValue(value = "timeout", omitEmpty = true)
  public Duration timeout;
  @UValue(value = "pretty", omitEmpty = true)
  public boolean pretty;
  @UValue(value = "timings", omitEmpty = true)
  public boolean timings;
  @UValue(value = "associative", omitEmpty = true)
  public boolean associative;
  @UValue(value = "blob_array", omitEmpty = true)
  public boolean blobAsArray;
  @UValue(value = "level", omitEmpty = true)
  public ReadConsistencyLevel level;
  @UValue(value = "linearizable_timeout", omitEmpty = true)
  public String linearizableTimeout;
  @UValue(value = "freshness", omitEmpty = true)
  public String freshness;
  @UValue(value = "freshness_strict", omitEmpty = true)
  public boolean freshnessStrict;
}
