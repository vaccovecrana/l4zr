package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.json.JsonObject;
import java.util.List;

import static io.vacco.l4zr.jdbc.L4Json.*;

public class L4Result {

  public List<String> columns;
  public List<String> types;
  public List<List<String>> values;
  public Long lastInsertId;
  public Integer rowsAffected;
  public String error;

  public L4Result(JsonObject json) {
    if (json.get("error") != null) {
      this.error = json.getString("error", "Unknown error");
    } else {
      this.columns = json.get("columns") != null ? toStringList(json.get("columns").asArray()) : null;
      this.types = json.get("types") != null ? toStringList(json.get("types").asArray()) : null;
      this.values = json.get("values") != null ? toValuesList(json.get("values").asArray()) : null;
      this.lastInsertId = json.get("last_insert_id") != null ? json.getLong("last_insert_id", -1) : null;
      this.rowsAffected = json.get("rows_affected") != null ? json.getInt("rows_affected", -1) : null;
    }
  }

  public boolean isError() {
    return error != null;
  }

}