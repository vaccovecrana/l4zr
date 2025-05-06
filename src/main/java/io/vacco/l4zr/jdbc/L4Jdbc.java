package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Result;
import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import static java.sql.Types.*;
import static java.lang.String.format;

public class L4Jdbc {

  public static final int VARCHAR_STREAM = Types.VARCHAR + 1000; // Custom type to distinguish stream
  public static final int UNICODE_STREAM = Types.VARCHAR + 1001; // Custom type for deprecated Unicode stream
  public static final int BINARY_STREAM = Types.BLOB + 1000;     // Custom type for binary stream
  public static final int CHARACTER_STREAM = Types.VARCHAR + 1002;
  public static final int CLOB_STREAM = Types.VARCHAR + 1003;
  public static final int OBJECT_STREAM = Types.OTHER + 1000;
  public static final int URL_STREAM = Types.DATALINK + 1000;
  public static final int NCLOB_STREAM = Types.NCLOB + 1000;
  public static final int NCHARACTER_STREAM = Types.NVARCHAR + 1000;

  // constants for rqlite types
  public static final String RQ_INTEGER = "INTEGER";
  public static final String RQ_NUMERIC = "NUMERIC";
  public static final String RQ_BOOLEAN = "BOOLEAN";
  public static final String RQ_TINYINT = "TINYINT";
  public static final String RQ_SMALLINT = "SMALLINT";
  public static final String RQ_BIGINT = "BIGINT";
  public static final String RQ_FLOAT = "FLOAT";
  public static final String RQ_DOUBLE = "DOUBLE";
  public static final String RQ_VARCHAR = "VARCHAR";
  public static final String RQ_DATE = "DATE";
  public static final String RQ_TIME = "TIME";
  public static final String RQ_TIMESTAMP = "TIMESTAMP";
  public static final String RQ_DATALINK = "DATALINK";
  public static final String RQ_CLOB = "CLOB";
  public static final String RQ_NCLOB = "NCLOB";
  public static final String RQ_NVARCHAR = "NVARCHAR";
  public static final String RQ_BLOB = "BLOB";
  public static final String RQ_NULL = "NULL";

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
        "Cannot convert value [%s], column %d (type %d) to (type %d)",
        value, columnIndex, sourceJdbcType, targetJdbcType
      ),
      SqlStateInvalidConversion
    );
  }

  public static boolean castBoolean(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == NUMERIC) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal == 0 || longVal == 1) {
          return longVal == 1;
        }
        rangeError(value, columnIndex, BOOLEAN);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid boolean format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    } else if (sourceJdbcType == VARCHAR || sourceJdbcType == BOOLEAN) {
      return Boolean.parseBoolean(value);
    }
    castError(value, columnIndex, sourceJdbcType, BOOLEAN);
    return false;
  }

  public static int castInteger(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT || sourceJdbcType == SMALLINT ||
      sourceJdbcType == BOOLEAN || sourceJdbcType == NUMERIC) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
          return (int) longVal;
        }
        rangeError(value, columnIndex, INTEGER);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid integer format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, INTEGER);
    return -1;
  }

  public static long castLong(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == BIGINT || sourceJdbcType == TINYINT ||
      sourceJdbcType == SMALLINT || sourceJdbcType == BOOLEAN || sourceJdbcType == NUMERIC) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid long format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, BIGINT);
    return -1;
  }

  public static float castFloat(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == FLOAT || sourceJdbcType == DOUBLE || sourceJdbcType == NUMERIC) {
      try {
        return Float.parseFloat(value);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid float format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, FLOAT);
    return -1;
  }

  public static double castDouble(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == FLOAT || sourceJdbcType == DOUBLE || sourceJdbcType == NUMERIC) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid double format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, DOUBLE);
    return -1;
  }

  public static byte castByte(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT || sourceJdbcType == BOOLEAN ||
      sourceJdbcType == NUMERIC) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Byte.MIN_VALUE && longVal <= Byte.MAX_VALUE) {
          return (byte) longVal;
        }
        rangeError(value, columnIndex, TINYINT);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid byte format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, TINYINT);
    return -1;
  }

  public static short castShort(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT || sourceJdbcType == SMALLINT ||
      sourceJdbcType == BOOLEAN || sourceJdbcType == NUMERIC) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Short.MIN_VALUE && longVal <= Short.MAX_VALUE) {
          return (short) longVal;
        }
        rangeError(value, columnIndex, SMALLINT);
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid short format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, SMALLINT);
    return -1;
  }

  public static BigDecimal castBigDecimal(String value, int columnIndex, int sourceJdbcType, int scale) throws SQLException {
    if (sourceJdbcType == INTEGER || sourceJdbcType == FLOAT || sourceJdbcType == DOUBLE ||
      sourceJdbcType == VARCHAR || sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN ||
      sourceJdbcType == TINYINT || sourceJdbcType == SMALLINT || sourceJdbcType == BIGINT) {
      try {
        var bd = new BigDecimal(value);
        if (scale != -1) {
          bd =  bd.setScale(scale, RoundingMode.HALF_UP);
        }
        return bd;
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid numeric format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, NUMERIC);
    return null;
  }

  public static InputStream castAsciiStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB || sourceJdbcType == NCLOB ||
      sourceJdbcType == NVARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN) {
      var asciiBytes = value.getBytes(StandardCharsets.US_ASCII); // Convert non-ASCII to '?'
      return new ByteArrayInputStream(asciiBytes);
    }
    castError(value, columnIndex, sourceJdbcType, VARCHAR_STREAM);
    return null;
  }

  public static InputStream castUnicodeStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB || sourceJdbcType == NCLOB ||
      sourceJdbcType == NVARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN) {
      var unicodeBytes = value.getBytes(StandardCharsets.UTF_16BE); // Encode as UTF-16BE
      return new ByteArrayInputStream(unicodeBytes);
    }
    castError(value, columnIndex, sourceJdbcType, UNICODE_STREAM);
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
    } else if (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB || sourceJdbcType == NCLOB ||
      sourceJdbcType == NVARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN) {
      return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)); // Encode as UTF-8
    }
    castError(value, columnIndex, sourceJdbcType, BINARY_STREAM);
    return null;
  }

  public static Reader castCharacterStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB || sourceJdbcType == NCLOB ||
      sourceJdbcType == NVARCHAR || sourceJdbcType == INTEGER || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN) {
      return new StringReader(value);
    }
    castError(value, columnIndex, sourceJdbcType, CHARACTER_STREAM);
    return null;
  }

  public static byte[] castBlob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == BLOB) {
      try {
        return Base64.getDecoder().decode(value);
      } catch (IllegalArgumentException e) {
        throw new SQLException(
          format("Base64 decoding error for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, BLOB);
    return null;
  }

  public static Clob castClob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB || sourceJdbcType == NCLOB ||
      sourceJdbcType == NVARCHAR) {
      return new SerialClob(value.toCharArray());
    }
    castError(value, columnIndex, sourceJdbcType, CLOB);
    return null;
  }

  public static Date castDate(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == DATE || sourceJdbcType == TIMESTAMP) {
      try {
        // Try parsing as ISO timestamp (e.g., "2023-10-15T00:00:00Z")
        try {
          var instant = Instant.parse(value); // Handles ISO 8601 with UTC (Z)
          var calendar = cal != null ? cal : Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          var zdt = instant.atZone(calendar.getTimeZone().toZoneId());
          return new Date(zdt.toInstant().toEpochMilli());
        } catch (DateTimeParseException e) {
          // Fallback to ISO local date (e.g., "2023-10-15")
          var localDate = LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
          var calendar = cal != null ? cal : Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          var zdt = localDate.atStartOfDay(calendar.getTimeZone().toZoneId());
          return new Date(zdt.toInstant().toEpochMilli());
        }
      } catch (DateTimeParseException e) {
        throw new SQLException(
          format("Invalid date format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Date(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid timestamp format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, DATE);
    return null;
  }

  public static Time castTime(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == TIME || sourceJdbcType == TIMESTAMP) {
      try {
        var localTime = LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
        var calendar = cal != null ? cal : Calendar.getInstance();
        var ldt = localTime.atDate(LocalDate.ofEpochDay(0)); // Epoch day for Time
        var zdt = ldt.atZone(calendar.getTimeZone().toZoneId());
        return new Time(zdt.toInstant().toEpochMilli());
      } catch (DateTimeParseException e) {
        throw new SQLException(
          format("Invalid time format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Time(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid timestamp format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, TIME);
    return null;
  }

  public static Timestamp castTimestamp(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == TIMESTAMP || sourceJdbcType == DATE) {
      try {
        // Try parsing as ISO timestamp (e.g., "2023-10-15T14:30:00Z")
        try {
          var instant = Instant.parse(value);
          return new Timestamp(instant.toEpochMilli());
        } catch (DateTimeParseException e) {
          // Fallback to ISO local date-time (e.g., "2023-10-15 14:30:00")
          var localDateTime = LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
          var calendar = cal != null ? cal : Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          var zdt = localDateTime.atZone(calendar.getTimeZone().toZoneId());
          return new Timestamp(zdt.toInstant().toEpochMilli());
        }
      } catch (DateTimeParseException e) {
        throw new SQLException(
          format("Invalid timestamp format for column %d: %s", columnIndex, value),
          L4Jdbc.SqlStateInvalidType, e
        );
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Timestamp(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw new SQLException(
          format("Invalid timestamp format for column %d: %s", columnIndex, value),
          L4Jdbc.SqlStateInvalidType, e
        );
      }
    }
    L4Jdbc.castError(value, columnIndex, sourceJdbcType, Types.TIMESTAMP);
    return null;
  }

  public static URL castURL(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == DATALINK) {
      try {
        return new URL(value);
      } catch (MalformedURLException e) {
        throw new SQLException(
          format("Invalid URL format for column %d: %s", columnIndex, value),
          SqlStateInvalidType, e
        );
      }
    }
    castError(value, columnIndex, sourceJdbcType, DATALINK);
    return null;
  }

  public static NClob castNClob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == NCLOB || sourceJdbcType == NVARCHAR ||
      sourceJdbcType == CLOB) {
      return new L4NClob(value.toCharArray());
    }
    castError(value, columnIndex, sourceJdbcType, NCLOB);
    return null;
  }

  public static Reader castNCharacterStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == VARCHAR || sourceJdbcType == NVARCHAR || sourceJdbcType == CLOB ||
      sourceJdbcType == NCLOB) {
      return new StringReader(value);
    }
    castError(value, columnIndex, sourceJdbcType, NCHARACTER_STREAM);
    return null;
  }

  public static <T> T castObject(String value, int columnIndex, int sourceJdbcType, Class<T> type) throws SQLException {
    if (type == null) {
      throw new SQLException("Target type cannot be null for column " + columnIndex, SqlStateInvalidType);
    }
    Object result;
    if (type == String.class && (sourceJdbcType == VARCHAR || sourceJdbcType == CLOB ||
      sourceJdbcType == NCLOB || sourceJdbcType == NVARCHAR)) {
      result = value;
    } else if (type == Integer.class && (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT ||
      sourceJdbcType == SMALLINT || sourceJdbcType == BOOLEAN ||
      sourceJdbcType == NUMERIC)) {
      result = castInteger(value, columnIndex, sourceJdbcType);
    } else if (type == Long.class && (sourceJdbcType == INTEGER || sourceJdbcType == BIGINT ||
      sourceJdbcType == TINYINT || sourceJdbcType == SMALLINT ||
      sourceJdbcType == BOOLEAN || sourceJdbcType == NUMERIC)) {
      result = castLong(value, columnIndex, sourceJdbcType);
    } else if (type == Float.class && (sourceJdbcType == FLOAT || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC)) {
      result = castFloat(value, columnIndex, sourceJdbcType);
    } else if (type == Double.class && (sourceJdbcType == FLOAT || sourceJdbcType == DOUBLE ||
      sourceJdbcType == NUMERIC)) {
      result = castDouble(value, columnIndex, sourceJdbcType);
    } else if (type == Byte.class && (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT ||
      sourceJdbcType == BOOLEAN || sourceJdbcType == NUMERIC)) {
      result = castByte(value, columnIndex, sourceJdbcType);
    } else if (type == Short.class && (sourceJdbcType == INTEGER || sourceJdbcType == TINYINT ||
      sourceJdbcType == SMALLINT || sourceJdbcType == BOOLEAN ||
      sourceJdbcType == NUMERIC)) {
      result = castShort(value, columnIndex, sourceJdbcType);
    } else if (type == BigDecimal.class && (sourceJdbcType == INTEGER || sourceJdbcType == FLOAT ||
      sourceJdbcType == DOUBLE || sourceJdbcType == VARCHAR ||
      sourceJdbcType == NUMERIC || sourceJdbcType == BOOLEAN ||
      sourceJdbcType == TINYINT || sourceJdbcType == SMALLINT ||
      sourceJdbcType == BIGINT)) {
      result = castBigDecimal(value, columnIndex, sourceJdbcType, -1);
    } else if (type == byte[].class && sourceJdbcType == BLOB) {
      result = castBlob(value, columnIndex, sourceJdbcType);
    } else {
      throw new SQLException(
        format("Cannot convert column %d (type %d) to %s", columnIndex, sourceJdbcType, type.getName()),
        SqlStateFeatureNotSupported
      );
    }
    return type.cast(result);
  }

  public static Object convertValue(String value, int sourceJdbcType, int targetJdbcType,
                                    int columnIndex, int scale, Calendar cal, Class<?> type) throws SQLException {
    try {
      switch (targetJdbcType) {
        case VARCHAR:
        case CHAR:              return value;
        case BOOLEAN:           return castBoolean(value, columnIndex, sourceJdbcType);
        case INTEGER:           return castInteger(value, columnIndex, sourceJdbcType);
        case BIGINT:            return castLong(value, columnIndex, sourceJdbcType);
        case DOUBLE:            return castDouble(value, columnIndex, sourceJdbcType);
        case FLOAT:             return castFloat(value, columnIndex, sourceJdbcType);
        case BLOB:              return castBlob(value, columnIndex, sourceJdbcType);
        case TINYINT:           return castByte(value, columnIndex, sourceJdbcType);
        case SMALLINT:          return castShort(value, columnIndex, sourceJdbcType);
        case DECIMAL:           return castBigDecimal(value, columnIndex, sourceJdbcType, scale);
        case DATE:              return castDate(value, columnIndex, sourceJdbcType, cal);
        case TIME:              return castTime(value, columnIndex, sourceJdbcType, cal);
        case TIMESTAMP:         return castTimestamp(value, columnIndex, sourceJdbcType, cal);
        case VARCHAR_STREAM:    return castAsciiStream(value, columnIndex, sourceJdbcType);
        case UNICODE_STREAM:    return castUnicodeStream(value, columnIndex, sourceJdbcType);
        case BINARY_STREAM:     return castBinaryStream(value, columnIndex, sourceJdbcType);
        case CHARACTER_STREAM:  return castCharacterStream(value, columnIndex, sourceJdbcType);
        case CLOB_STREAM:       return castClob(value, columnIndex, sourceJdbcType);
        case OBJECT_STREAM:     return castObject(value, columnIndex, sourceJdbcType, type);
        case URL_STREAM:        return castURL(value, columnIndex, sourceJdbcType);
        case NCLOB_STREAM:      return castNClob(value, columnIndex, sourceJdbcType);
        case NCHARACTER_STREAM: return castNCharacterStream(value, columnIndex, sourceJdbcType);
        case NULL:              return null;
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

  public static int getJdbcType(String rqliteType) {
    if (rqliteType == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    var rqType = rqliteType.trim().toUpperCase();
    switch (rqType) {
      case RQ_INTEGER:    return Types.INTEGER;
      case RQ_NUMERIC:    return Types.NUMERIC;
      case RQ_BOOLEAN:    return Types.BOOLEAN;
      case RQ_TINYINT:    return Types.TINYINT;
      case RQ_SMALLINT:   return Types.SMALLINT;
      case RQ_BIGINT:     return Types.BIGINT;
      case RQ_FLOAT:      return Types.FLOAT;
      case RQ_DOUBLE:     return Types.DOUBLE;
      case RQ_VARCHAR:    return Types.VARCHAR;
      case RQ_DATE:       return Types.DATE;
      case RQ_TIME:       return Types.TIME;
      case RQ_TIMESTAMP:  return Types.TIMESTAMP;
      case RQ_DATALINK:   return Types.DATALINK;
      case RQ_CLOB:       return Types.CLOB;
      case RQ_NCLOB:      return Types.NCLOB;
      case RQ_NVARCHAR:   return Types.NVARCHAR;
      case RQ_BLOB:       return Types.BLOB;
      case RQ_NULL:       return Types.NULL;
      default: return -1;
    }
  }

}
