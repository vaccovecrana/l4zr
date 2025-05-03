package rqlite.schema.commands;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RequestResponse {
  @JsonProperty("results")
  public Object results; // may be List<RequestResult> or List<RequestResultAssoc>
  @JsonProperty("time")
  public double time;

  public boolean hasError() {
    if (results instanceof List<?>) {
      List<?> list = (List<?>) results;
      if (!list.isEmpty()) {
        Object first = list.get(0);
        if (first instanceof RequestResult) {
          for (Object o : list) {
            RequestResult rr = (RequestResult) o;
            if (rr.error != null && !rr.error.isEmpty()) {
              return true;
            }
          }
        } else if (first instanceof RequestResultAssoc) {
          for (Object o : list) {
            RequestResultAssoc rra = (RequestResultAssoc) o;
            if (rra.error != null && !rra.error.isEmpty()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }
}
