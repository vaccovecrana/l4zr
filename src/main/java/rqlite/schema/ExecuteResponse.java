package rqlite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/* Response types. Fields are annotated for Jackson. */
public class ExecuteResponse {
  @JsonProperty("results")
  public List<ExecuteResult> results;
  @JsonProperty("time")
  public double time;
  @JsonProperty("sequence_number")
  public long sequenceNumber;

  public boolean hasError() {
    if (results != null) {
      for (ExecuteResult res : results) {
        if (res.error != null && !res.error.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }
}
