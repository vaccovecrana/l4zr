package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.json.*;
import java.util.*;

public class L4Statement {

  private String sql;
  private List<Object> positionalParams = new ArrayList<>();
  private Map<String, Object> namedParams;

  public L4Statement sql(String sql) {
    this.sql = sql;
    return this;
  }

  public L4Statement withPositionalParam(Object param) {
    if (namedParams != null && !namedParams.isEmpty()) {
      throw new IllegalStateException("Cannot mix positional and named parameters in the same statement");
    }
    this.positionalParams.add(param);
    return this;
  }

  public L4Statement withPositionalParams(Object ... params) {
    if (namedParams != null && !namedParams.isEmpty()) {
      throw new IllegalStateException("Cannot mix positional and named parameters in the same statement");
    }
    this.positionalParams = Arrays.asList(params);
    return this;
  }

  public L4Statement withNamedParam(String name, Object value) {
    if (!positionalParams.isEmpty()) {
      throw new IllegalStateException("Cannot mix positional and named parameters in the same statement");
    }
    if (this.namedParams == null) {
      this.namedParams = new java.util.HashMap<>();
    }
    this.namedParams.put(name, value);
    return this;
  }

  public L4Statement withNamedParams(Map<String, Object> params) {
    if (!positionalParams.isEmpty()) {
      throw new IllegalStateException("Cannot mix positional and named parameters in the same statement");
    }
    this.namedParams = new java.util.HashMap<>(params);
    return this;
  }

  private JsonValue toJsonValue(Object value) {
    if (value == null) {
      return Json.NULL;
    } else if (value instanceof String) {
      return Json.value((String) value);
    } else if (value instanceof Integer) {
      return Json.value((Integer) value);
    } else if (value instanceof Long) {
      return Json.value((Long) value);
    } else if (value instanceof Double) {
      return Json.value((Double) value);
    } else if (value instanceof Float) {
      return Json.value((Float) value);
    } else if (value instanceof Boolean) {
      return Json.value((Boolean) value);
    } else if (value instanceof byte[]) {
      return Json.value(java.util.Base64.getEncoder().encodeToString((byte[]) value));
    } else {
      return Json.value(value.toString());
    }
  }

  public JsonArray build() {
    if (sql == null || sql.trim().isEmpty()) {
      throw new IllegalStateException("SQL statement cannot be null or empty");
    }
    var out = new JsonArray();
    out.add(sql);
    if (namedParams != null && !namedParams.isEmpty()) {
      var paramsObject = new JsonObject();
      for (var entry : namedParams.entrySet()) {
        paramsObject.add(entry.getKey(), toJsonValue(entry.getValue()));
      }
      out.add(paramsObject);
    } else if (!positionalParams.isEmpty()) {
      for (var param : positionalParams) {
        out.add(toJsonValue(param));
      }
    }
    return out;
  }

  public static JsonValue toArray(L4Statement ... statements) {
    var smtList = Json.array();
    for (var smt : statements) {
      smtList.add(smt.build());
    }
    return smtList;
  }

}
