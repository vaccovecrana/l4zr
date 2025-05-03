package rqlite.schema.options;

import java.time.Duration;

public class ExecuteOptions {
  @UValue(value = "transaction", omitEmpty = true)
  public boolean transaction;
  @UValue(value = "pretty", omitEmpty = true)
  public boolean pretty;
  @UValue(value = "timings", omitEmpty = true)
  public boolean timings;
  @UValue(value = "queue", omitEmpty = true)
  public boolean queue;
  @UValue(value = "wait", omitEmpty = true)
  public boolean wait;
  @UValue(value = "timeout", omitEmpty = true)
  public Duration timeout;
}
