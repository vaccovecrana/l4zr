package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;

import static io.vacco.l4zr.jdbc.L4Jdbc.*;
import static java.sql.Types.*;

public class L4RsMeta implements ResultSetMetaData {

  private final L4Result result;

  public L4RsMeta(L4Result result) {
    this.result = Objects.requireNonNull(result);
  }

  @Override public int getColumnCount() {
    return result.columns.size();
  }

  /*
   * rqlite does not provide direct metadata to determine if a column is auto-incrementing.
   * A proper implementation would require querying the schema (e.g., PRAGMA table_info)
   * and checking if the column is an INTEGER PRIMARY KEY with AUTOINCREMENT.
   * For now, return false to indicate no columns are auto-incrementing.
   * This may cause applications to attempt to insert values into auto-increment columns,
   * which will fail if the column is an INTEGER PRIMARY KEY without AUTOINCREMENT.
   */
  @Override public boolean isAutoIncrement(int column) throws SQLException {
    checkColumn(column, result);
    return false;
  }

  /*
   * rqlite (SQLite) is generally case-insensitive for string comparisons by default (NOCASE collation).
   * Case sensitivity can be enabled with a COLLATE BINARY clause, but rqlite does not expose
   * collation metadata in query results. A proper implementation would require querying the schema
   * (e.g., SELECT sql FROM sqlite_master)and parsing the CREATE TABLE statement
   * to check for COLLATE clauses. For now, assume case-insensitive behavior for strings (the SQLite default) and return false.
   * This may cause issues if a column uses a case-sensitive collation (e.g., COLLATE BINARY),
   * as applications might assume case-insensitive comparisons.
   */
  @Override public boolean isCaseSensitive(int column) throws SQLException {
    checkColumn(column, result);
    return false;
  }

  /* SQLite/rqlite columns are generally searchable in WHERE clauses. */
  @Override public boolean isSearchable(int column) throws SQLException {
    checkColumn(column, result);
    return true;
  }

  /* SQLite/rqlite has no dedicated currency type or metadata. */
  @Override public boolean isCurrency(int column) throws SQLException {
    checkColumn(column, result);
    return false;
  }

  /* Nullability is unknown without schema metadata. */
  @Override public int isNullable(int column) throws SQLException {
    checkColumn(column, result);
    return ResultSetMetaData.columnNullableUnknown;
  }

  @Override public boolean isSigned(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1);
    if (type == null) {
      return false; // Fallback for missing type metadata (should be rare).
    }
    var typeUpper = type.toUpperCase();
    return typeUpper.equals(RqInteger) ||
      typeUpper.equals(RqReal) ||
      typeUpper.equals(RqNumeric);   // In case rqlite reports DOUBLE.
  }

  @Override public int getColumnDisplaySize(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1);
    if (type == null) {
      return 4; // Fallback for NULL or unknown types (displays as "NULL").
    }
    var typeUpper = type.toUpperCase();
    switch (typeUpper) {
      case RqInteger: return 20; // Covers -9223372036854775808 (19 digits + sign).
      case RqReal:    return 25; // Covers scientific notation (e.g., -1.23456789012345E+308).
      case RqText:
      case RqBlob:    return 255; // Conservative default for strings or blobs.
      default:        return 4; // Fallback for NULL or unknown types.
    }
  }

  @Override public String getColumnLabel(int column) throws SQLException {
    checkColumn(column, result);
    return this.result.columns.get(column - 1);
  }

  /* Fallback: returns alias or name due to lack of schema metadata. */
  @Override public String getColumnName(int column) throws SQLException {
    checkColumn(column, result);
    return this.result.columns.get(column - 1);
  }

  /* SQLite/rqlite has no schema namespaces; empty string is standard. */
  @Override public String getSchemaName(int column) throws SQLException {
    checkColumn(column, result);
    return "";
  }

  @Override public int getPrecision(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1);
    if (type == null) {
      return 0; // Fallback for NULL or unknown types.
    }
    var typeUpper = type.toUpperCase();
    switch (typeUpper) {
      case RqInteger:
      case RqNumeric: return 19; // Max digits in 64-bit signed integer.
      case RqReal:    return 15; // Approx. significant digits in IEEE 754 double.
      case RqText:
      case RqBlob:    return 255; // Conservative default for strings or blobs.
      default:        return 0; // Fallback for unknown types.
    }
  }

  /* SQLite types have no fixed decimal places. */
  @Override public int getScale(int column) throws SQLException {
    checkColumn(column, result);
    return 0;
  }

  /* Table name unknown without query parsing or schema metadata. */
  @Override public String getTableName(int column) throws SQLException {
    checkColumn(column, result);
    return "";
  }

  /* SQLite/rqlite has no catalogs; empty string is standard. */
  @Override public String getCatalogName(int column) throws SQLException {
    checkColumn(column, result);
    return "";
  }

  @Override public int getColumnType(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1);
    if (type == null) {
      return OTHER; // Fallback for unknown or NULL types.
    }
    var typeUpper = type.toUpperCase();
    switch (typeUpper) {
      case RqInteger: return BIGINT; // 64-bit signed integer.
      case RqReal:    return DOUBLE; // Double-precision float.
      case RqText:    return VARCHAR; // Variable-length string.
      case RqBlob:    return BLOB; // Binary data.
      case RqNumeric: return NUMERIC; // Flexible numeric type.
      default:        return OTHER; // Fallback for unrecognized types.
    }
  }

  /* Return native type name */
  @Override public String getColumnTypeName(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1);
    return type != null ? type : "";
  }

  /* Assume columns might be writable without metadata. */
  @Override public boolean isReadOnly(int column) throws SQLException {
    checkColumn(column, result);
    return false;
  }

  /* Assume columns are potentially writable, consistent with isReadOnly = false. */
  @Override public boolean isWritable(int column) throws SQLException {
    checkColumn(column, result);
    return true;
  }

  /* Cannot guarantee write without metadata. */
  @Override public boolean isDefinitelyWritable(int column) throws SQLException {
    checkColumn(column, result);
    return false;
  }

  @Override public String getColumnClassName(int column) throws SQLException {
    checkColumn(column, result);
    var type = result.types.get(column - 1); // JDBC is 1-based, List is 0-based.
    if (type == null) {
      return Object.class.getCanonicalName(); // Fallback for unknown or NULL types.
    }
    var typeUpper = type.toUpperCase();
    switch (typeUpper) {
      case RqInteger: return Long.class.getCanonicalName(); // 64-bit signed integer.
      case RqReal:    return Double.class.getCanonicalName(); // Double-precision float.
      case RqText:    return String.class.getCanonicalName(); // Variable-length string.
      case RqBlob:    return byte[].class.getCanonicalName(); // Binary data.
      case RqNumeric: return BigDecimal.class.getCanonicalName(); // Flexible numeric type.
      default:        return Object.class.getCanonicalName(); // Fallback for unrecognized types.
    }
  }

  /* Support ResultSetMetaData and Wrapper. */
  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException("Interface cannot be null");
    }
    if (iface == ResultSetMetaData.class || iface == Wrapper.class) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException("Interface cannot be null");
    }
    return iface == ResultSetMetaData.class || iface == Wrapper.class;
  }

}
