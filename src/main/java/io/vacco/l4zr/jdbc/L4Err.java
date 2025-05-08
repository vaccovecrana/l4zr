package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;

import java.sql.SQLException;

import static java.lang.String.format;

public class L4Err {

  public static final String
    SqlStateInvalidParam        = "22003",
    SqlStateInvalidConversion   = "22018",
    SqlStateGeneralError        = "HY000",
    SqlStateInvalidColumn       = "22003",
    SqlStateInvalidCursor       = "24000",
    SqlStateFeatureNotSupported = "0A000",
    SqlStateInvalidAttr         = "HY092",
    SqlStateInvalidType         = "22005",
    SqlStateInvalidQuery        = "42000",
    SqlStateConnectionError     = "08S01";

  // TODO add other `throw new SQLException` errors as methods here.

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
      throw new SQLException("ResultSet is closed", SqlStateGeneralError);
    }
    if (currentRow < 0 || currentRow >= result.values.size()) {
      throw new SQLException("Invalid row position: " + (currentRow + 1), SqlStateInvalidCursor);
    }
  }

  public static void rangeError(String value, int columnIndex, int jdbcType) throws SQLException {
    throw new SQLException(
      format("Value [%s] out of range for JDBC type [%d] in column %d", value, jdbcType, columnIndex),
      SqlStateInvalidType
    );
  }

  public static void castError(String value, int columnIndex, int sourceJdbcType, int targetJdbcType) throws SQLException {
    throw new SQLException(
      format(
        "Cannot convert value [%s], column %d (type %d) to (type %d)",
        value, columnIndex, sourceJdbcType, targetJdbcType
      ),
      SqlStateInvalidConversion
    );
  }

}
