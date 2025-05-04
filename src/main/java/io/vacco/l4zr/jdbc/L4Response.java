package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class L4Response {

  public List<L4Result> results;
  public Float time;
  public int statusCode;

  public L4Response(int statusCode, JsonObject obj) {
    this.statusCode = statusCode;
    var resultsArray = obj.get("results").asArray();
    this.results = new ArrayList<>();
    for (var resultValue : resultsArray) {
      this.results.add(new L4Result(resultValue.asObject()));
    }
    this.time = obj.get("time") != null ? obj.getFloat("time", -1) : null;
  }

}