package rqlite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class QueryResultAssoc {
  @JsonProperty("types")
  public Map<String, String> types;
  @JsonProperty("rows")
  public List<Map<String, Object>> rows;
  @JsonProperty("time")
  public double time;
  @JsonProperty("error")
  public String error;
}
