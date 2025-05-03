package rqlite.schema.commands;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExecuteResult {
  @JsonProperty("last_insert_id")
  public long lastInsertID;
  @JsonProperty("rows_affected")
  public long rowsAffected;
  @JsonProperty("time")
  public double time;
  @JsonProperty("error")
  public String error;
}
