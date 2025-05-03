package rqlite.schema.commands;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class QueryResponse {
  @JsonProperty("results")
  public Object results; // may be List<QueryResult> or List<QueryResultAssoc>
  @JsonProperty("time")
  public double time;

  public boolean hasError() {
    if (results instanceof List<?>) {
      List<?> list = (List<?>) results;
      if (!list.isEmpty()) {
        Object first = list.get(0);
        if (first instanceof QueryResult) {
          for (Object o : list) {
            QueryResult qr = (QueryResult) o;
            if (qr.error != null && !qr.error.isEmpty()) {
              return true;
            }
          }
        } else if (first instanceof QueryResultAssoc) {
          for (Object o : list) {
            QueryResultAssoc qra = (QueryResultAssoc) o;
            if (qra.error != null && !qra.error.isEmpty()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }
}
