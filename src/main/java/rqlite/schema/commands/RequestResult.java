package rqlite.schema.commands;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

class RequestResult {
  @JsonProperty("columns")
  public List<String> columns;
  @JsonProperty("types")
  public List<String> types;
  @JsonProperty("values")
  public List<List<Object>> values;
  @JsonProperty("last_insert_id")
  public Long lastInsertID;
  @JsonProperty("rows_affected")
  public Long rowsAffected;
  @JsonProperty("error")
  public String error;
  @JsonProperty("time")
  public double time;
}
