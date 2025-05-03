package rqlite.schema;

import java.time.Duration;

public class NodeOptions {
  @UValue(value = "timeout", omitEmpty = true)
  public Duration timeout;
  @UValue(value = "pretty", omitEmpty = true)
  public boolean pretty;
  @UValue(value = "non_voters", omitEmpty = true)
  public boolean nonVoters;
  @UValue(value = "ver", omitEmpty = true)
  public String version;
}
