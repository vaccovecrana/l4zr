package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;
import java.sql.SQLException;

public class L4Jdbc {

  public static final String
    RqInteger = "INTEGER", RqNumeric = "NUMERIC",
    RqReal = "REAL", RqText = "TEXT", RqBlob = "BLOB";

  public static final String
    SqlStateInvalidParam = "22003",
    SqlStateInvalidConversion = "22018",
    SqlStateClosed = "HY000",
    SqlStateInvalidCursor = "24000",
    SqlStateFeatureNotSupported = "0A000";

  public static void checkColumn(int idx, L4Result result) throws SQLException {
    if (idx < 1 || idx > result.columns.size()) {
      throw new SQLException("Invalid column index: " + idx, "22003");
    }
  }

  public static void checkColumnLabel(String label, L4Result result) throws SQLException {
    if (label == null || !result.columns.contains(label)) {
      throw new SQLException("Invalid column label: " + label, SqlStateInvalidParam);
    }
  }

  public static void checkRow(int currentRow, L4Result result, boolean isClosed) throws SQLException {
    if (isClosed) {
      throw new SQLException("ResultSet is closed", SqlStateClosed);
    }
    if (currentRow < 0 || currentRow >= result.values.size()) {
      throw new SQLException("Invalid row position: " + (currentRow + 1), SqlStateInvalidCursor);
    }
  }

}
