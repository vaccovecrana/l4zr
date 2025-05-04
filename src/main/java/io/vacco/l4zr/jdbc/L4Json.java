package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

public class L4Json {

  public static List<String> toStringList(JsonArray array) {
    var list = new ArrayList<String>();
    for (var value : array) {
      list.add(value.asString());
    }
    return list;
  }

  public static List<List<String>> toValuesList(JsonArray valuesArray) {
    var values = new ArrayList<List<String>>();
    for (var rowValue : valuesArray) {
      var row = rowValue.asArray();
      var rowValues = new ArrayList<String>();
      for (int i = 0; i < row.size(); i++) {
        if (row.get(i).isString()) {
          rowValues.add(row.get(i).asString());
        } else {
          rowValues.add(row.get(i).toString());
        }
      }
      values.add(rowValues);
    }
    return values;
  }

}
