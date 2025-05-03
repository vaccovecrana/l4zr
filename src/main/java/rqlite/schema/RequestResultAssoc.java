package rqlite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

class RequestResultAssoc {
  @JsonProperty("types")
  public Map<String, String> types;
  @JsonProperty("rows")
  public List<Map<String, Object>> rows;
  @JsonProperty("last_insert_id")
  public Long lastInsertID;
  @JsonProperty("rows_affected")
  public Long rowsAffected;
  @JsonProperty("error")
  public String error;
  @JsonProperty("time")
  public double time;
}
