package rqlite.schema.commands;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class QueryResult {
  @JsonProperty("columns")
  public List<String> columns;
  @JsonProperty("types")
  public List<String> types;
  @JsonProperty("values")
  public List<List<Object>> values;
  @JsonProperty("time")
  public double time;
  @JsonProperty("error")
  public String error;
}
