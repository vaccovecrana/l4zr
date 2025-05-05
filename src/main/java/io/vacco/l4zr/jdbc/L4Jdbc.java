package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;
import java.sql.SQLException;

public class L4Jdbc {

  public static final String
    RqInteger = "INTEGER", RqNumeric = "NUMERIC",
    RqReal = "REAL", RqText = "TEXT", RqBlob = "BLOB";

  public static void checkColumn(int idx, L4Result result) throws SQLException {
    if (idx < 1 || idx > result.columns.size()) {
      throw new SQLException("Invalid column index: " + idx, "22003");
    }
  }

}
