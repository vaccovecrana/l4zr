package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;
import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import static java.sql.Types.*;
import static java.lang.String.format;

public class L4Jdbc {

  public static final int VARCHAR_STREAM = Types.VARCHAR + 1000; // Custom type to distinguish stream
  public static final int UNICODE_STREAM = Types.VARCHAR + 1001; // Custom type for deprecated Unicode stream
  public static final int BINARY_STREAM = Types.BLOB + 1000;     // Custom type for binary stream
  public static final int CHARACTER_STREAM = Types.VARCHAR + 1002;
  public static final int CLOB_STREAM = Types.VARCHAR + 1003;

  public static final String
    RqInteger = "INTEGER", RqNumeric = "NUMERIC",
    RqReal = "REAL", RqText = "TEXT", RqBlob = "BLOB";

  public static final String
    SqlStateInvalidParam = "22003",
    SqlStateInvalidConversion = "22018",
    SqlStateClosed = "HY000",
    SqlStateInvalidColumn = "22003",
    SqlStateInvalidCursor = "24000",
    SqlStateFeatureNotSupported = "0A000",
    SqlStateInvalidAttr = "HY092",
    SqlStateInvalidType = "22005";;

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

  private static void rangeError(String value, int columnIndex, int jdbcType) throws SQLException {
    throw new SQLException(
      format("Value [%s] out of range for JDBC type [%d] in column %d", value, jdbcType, columnIndex),
      SqlStateInvalidType
    );
  }

  private static void castError(String value, int columnIndex, int sourceJdbcType, int targetJdbcType) throws SQLException {
    throw new SQLException(
      format(
        "Cannot convert value [%s], column %d (type %d) to %d",
        value, columnIndex, sourceJdbcType, targetJdbcType
      ),
      SqlStateInvalidConversion
    );
  }

  public static boolean castBoolean(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER) {
      var longVal = Long.parseLong(value);
      if (longVal == 0 || longVal == 1) {
        return longVal == 1;
      }
    } else if (sourceJdbcType == VARCHAR) {
      return Boolean.parseBoolean(value);
    }
    rangeError(value, columnIndex, BOOLEAN);
    return false;
  }

  public static int castInteger(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER) {
      var longVal = Long.parseLong(value);
      if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
        return (int) longVal;
      }
      rangeError(value, columnIndex, INTEGER);
    }
    castError(value, columnIndex, sourceJdbcType, INTEGER);
    return -1;
  }

  public static long castLong(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER) {
      return Long.parseLong(value);
    }
    castError(value, columnIndex, sourceJdbcType, BIGINT);
    return -1;
  }

  public static double castDouble(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == REAL) {
      return Double.parseDouble(value);
    }
    castError(value, columnIndex, sourceJdbcType, DOUBLE);
    return -1;
  }

  public static byte castByte(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER) {
      long longVal = Long.parseLong(value);
      if (longVal >= Byte.MIN_VALUE && longVal <= Byte.MAX_VALUE) {
        return (byte) longVal;
      }
      rangeError(value, columnIndex, TINYINT);
    }
    castError(value, columnIndex, sourceJdbcType, TINYINT);
    return -1;
  }

  public static short castShort(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER) {
      long longVal = Long.parseLong(value);
      if (longVal >= Short.MIN_VALUE && longVal <= Short.MAX_VALUE) {
        return (short) longVal;
      }
      rangeError(value, columnIndex, SMALLINT);
    }
    castError(value, columnIndex, sourceJdbcType, SMALLINT);
    return -1;
  }

  public static BigDecimal castBigDecimal(String value, int columnIndex, int sourceJdbcType, int scale) throws SQLException {
    if (scale < 0) {
      throw new SQLException(
        format("Invalid scale %d for column %d", scale, columnIndex),
        SqlStateInvalidType
      );
    }
    if (sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE || sourceJdbcType == VARCHAR) {
      try {
        var bd = new BigDecimal(value);
        return bd.setScale(scale, RoundingMode.HALF_UP);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid numeric format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, DECIMAL);
    return null;
  }

  public static InputStream castAsciiStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE) {
      var asciiBytes = value.getBytes(StandardCharsets.US_ASCII); // Convert non-ASCII characters to '?' (ASCII fallback)
      return new ByteArrayInputStream(asciiBytes);
    }
    castError(value, columnIndex, sourceJdbcType, VARCHAR);
    return null;
  }

  public static InputStream castUnicodeStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE) {
      var unicodeBytes = value.getBytes(StandardCharsets.UTF_16BE); // Encode as UTF-16BE (JDBC standard for getUnicodeStream)
      return new ByteArrayInputStream(unicodeBytes);
    }
    castError(value, columnIndex, sourceJdbcType, VARCHAR);
    return null;
  }

  public static InputStream castBinaryStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == BLOB) {
      try {
        var bytes = Base64.getDecoder().decode(value);
        return new ByteArrayInputStream(bytes);
      } catch (IllegalArgumentException e) {
        throw new SQLException(
          format("Base64 decoding error for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    } else if (sourceJdbcType == VARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE) {
      return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)); // Encode TEXT/INTEGER/REAL as UTF-8
    }
    castError(value, columnIndex, sourceJdbcType, BLOB);
    return null;
  }

  public static Reader castCharacterStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE) {
      return new StringReader(value);
    }
    castError(value, columnIndex, sourceJdbcType, VARCHAR);
    return null;
  }

  public static byte[] castBlob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == BLOB) {
      return Base64.getDecoder().decode(value);
    }
    castError(value, columnIndex, sourceJdbcType, BLOB);
    return null;
  }

  public static Clob castClob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR) {
      return new SerialClob(value.toCharArray());
    }
    castError(value, columnIndex, sourceJdbcType, CLOB);
    return null;
  }

  public static Date castDate(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR) {
      var localDate = LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
      return Date.valueOf(localDate);
    } else if (sourceJdbcType == INTEGER) {
      var seconds = Long.parseLong(value);
      return new Date(seconds * 1000L);
    }
    castError(value, columnIndex, sourceJdbcType, DATE);
    return null;
  }

  public static Time castTime(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR) {
      var localTime = LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
      return Time.valueOf(localTime);
    } else if (sourceJdbcType == INTEGER) {
      var seconds = Long.parseLong(value);
      return new Time(seconds * 1000L);
    }
    castError(value, columnIndex, sourceJdbcType, TIME);
    return null;
  }

  public static Timestamp castTimestamp(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR) {
      var localDateTime = LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd[[' ']['T']HH:mm:ss][.SSSSSSSSS][.SSSSSS][.SSS]"));
      return Timestamp.valueOf(localDateTime);
    } else if (sourceJdbcType == INTEGER) {
      var seconds = Long.parseLong(value);
      return new Timestamp(seconds * 1000L);
    } else if (sourceJdbcType == DOUBLE) {
      double millis = Double.parseDouble(value) * 1000.0;
      return new Timestamp((long) millis);
    }
    castError(value, columnIndex, sourceJdbcType, TIMESTAMP);
    return null;
  }

  public static Object convertValue(String value, int sourceJdbcType, int targetJdbcType, int columnIndex, int scale) throws SQLException {
    try {
      switch (targetJdbcType) {
        case VARCHAR:
        case CHAR:              return value;
        case BOOLEAN:           return castBoolean(value, columnIndex, sourceJdbcType);
        case INTEGER:           return castInteger(value, columnIndex, sourceJdbcType);
        case BIGINT:            return castLong(value, columnIndex, sourceJdbcType);
        case DOUBLE:
        case FLOAT:             return castDouble(value, columnIndex, sourceJdbcType);
        case BLOB:              return castBlob(value, columnIndex, sourceJdbcType);
        case TINYINT:           return castByte(value, columnIndex, sourceJdbcType);
        case SMALLINT:          return castShort(value, columnIndex, sourceJdbcType);
        case DECIMAL:           return castBigDecimal(value, columnIndex, sourceJdbcType, scale);
        case DATE:              return castDate(value, columnIndex, sourceJdbcType);
        case TIME:              return castTime(value, columnIndex, sourceJdbcType);
        case TIMESTAMP:         return castTimestamp(value, columnIndex, sourceJdbcType);
        case VARCHAR_STREAM:    return castAsciiStream(value, columnIndex, sourceJdbcType);
        case UNICODE_STREAM:    return castUnicodeStream(value, columnIndex, sourceJdbcType);
        case BINARY_STREAM:     return castBinaryStream(value, columnIndex, sourceJdbcType);
        case CHARACTER_STREAM:  return castCharacterStream(value, columnIndex, sourceJdbcType);
        case CLOB_STREAM:       return castClob(value, columnIndex, sourceJdbcType);
        default:
          throw new SQLException(
            format("Unsupported JDBC type %d for column %d", targetJdbcType, columnIndex),
            SqlStateFeatureNotSupported
          );
      }
    } catch (Exception e) {
      throw new SQLException(
        format("Conversion error for column %d: %s", columnIndex, value),
        SqlStateInvalidConversion, e
      );
    }
  }

}
