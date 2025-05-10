package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Statement;
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

import static io.vacco.l4zr.jdbc.L4Err.*;
import static java.sql.Types.*;
import static java.lang.String.format;

public class L4Jdbc {

  public static final int VARCHAR_STREAM    = Types.VARCHAR + 1000; // Custom type to distinguish stream
  public static final int UNICODE_STREAM    = Types.VARCHAR + 1001; // Custom type for deprecated Unicode stream
  public static final int BINARY_STREAM     = Types.BLOB + 1000;     // Custom type for binary stream
  public static final int CHARACTER_STREAM  = Types.VARCHAR + 1002;
  public static final int CLOB_STREAM       = Types.VARCHAR + 1003;
  public static final int OBJECT_STREAM     = Types.OTHER + 1000;
  public static final int URL_STREAM        = Types.DATALINK + 1000;
  public static final int NCLOB_STREAM      = Types.NCLOB + 1000;
  public static final int NCHARACTER_STREAM = Types.NVARCHAR + 1000;

  // constants for rqlite types
  public static final String RQ_INTEGER   = "INTEGER";
  public static final String RQ_NUMERIC   = "NUMERIC";
  public static final String RQ_BOOLEAN   = "BOOLEAN";
  public static final String RQ_TINYINT   = "TINYINT";
  public static final String RQ_SMALLINT  = "SMALLINT";
  public static final String RQ_BIGINT    = "BIGINT";
  public static final String RQ_FLOAT     = "FLOAT";
  public static final String RQ_DOUBLE    = "DOUBLE";
  public static final String RQ_VARCHAR   = "VARCHAR";
  public static final String RQ_DATE      = "DATE";
  public static final String RQ_TIME      = "TIME";
  public static final String RQ_TIMESTAMP = "TIMESTAMP";
  public static final String RQ_DATALINK  = "DATALINK";
  public static final String RQ_CLOB      = "CLOB";
  public static final String RQ_NCLOB     = "NCLOB";
  public static final String RQ_NVARCHAR  = "NVARCHAR";
  public static final String RQ_BLOB      = "BLOB";
  public static final String RQ_NULL      = "NULL";

  public static boolean anyOf(int sourceType, int ... types) {
    for (var t : types) {
      if (sourceType == t) {
        return true;
      }
    }
    return false;
  }

  public static boolean castBoolean(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, INTEGER, NUMERIC)) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal == 0 || longVal == 1) {
          return longVal == 1;
        }
        throw rangeError(value, columnIndex, BOOLEAN);
      } catch (NumberFormatException e) {
        throw badBoolean(columnIndex, value, e);
      }
    } else if (anyOf(sourceJdbcType, VARCHAR, BOOLEAN)) {
      return Boolean.parseBoolean(value);
    }
    throw castError(value, columnIndex, sourceJdbcType, BOOLEAN);
  }

  public static int castInteger(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, INTEGER, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
          return (int) longVal;
        }
        throw rangeError(value, columnIndex, INTEGER);
      } catch (NumberFormatException e) {
        throw badInteger(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, INTEGER);
  }

  public static long castLong(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, INTEGER, BIGINT, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw badLong(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, BIGINT);
  }

  public static float castFloat(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, FLOAT, DOUBLE, NUMERIC)) {
      try {
        return Float.parseFloat(value);
      } catch (NumberFormatException e) {
        throw badFloat(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, FLOAT);
  }

  public static double castDouble(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, FLOAT, DOUBLE, NUMERIC)) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        throw badDouble(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, DOUBLE);
  }

  public static byte castByte(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType,  INTEGER, TINYINT, BOOLEAN, NUMERIC)) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Byte.MIN_VALUE && longVal <= Byte.MAX_VALUE) {
          return (byte) longVal;
        }
        throw rangeError(value, columnIndex, TINYINT);
      } catch (NumberFormatException e) {
        throw badByte(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, TINYINT);
  }

  public static short castShort(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, INTEGER, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      try {
        var longVal = Long.parseLong(value);
        if (longVal >= Short.MIN_VALUE && longVal <= Short.MAX_VALUE) {
          return (short) longVal;
        }
        throw rangeError(value, columnIndex, SMALLINT);
      } catch (NumberFormatException e) {
        throw badShort(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, SMALLINT);
  }

  public static BigDecimal castBigDecimal(String value, int columnIndex, int sourceJdbcType, int scale) throws SQLException {
    if (anyOf(sourceJdbcType, INTEGER, FLOAT, DOUBLE, VARCHAR, NUMERIC, BOOLEAN, TINYINT, SMALLINT, BIGINT)) {
      try {
        var bd = new BigDecimal(value);
        if (scale != -1) {
          bd =  bd.setScale(scale, RoundingMode.HALF_UP);
        }
        return bd;
      } catch (NumberFormatException e) {
        throw badBigDecimal(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, NUMERIC);
  }

  public static InputStream castAsciiStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR, INTEGER, DOUBLE, NUMERIC, BOOLEAN)) {
      var asciiBytes = value.getBytes(StandardCharsets.US_ASCII); // Convert non-ASCII to '?'
      return new ByteArrayInputStream(asciiBytes);
    }
    throw castError(value, columnIndex, sourceJdbcType, VARCHAR_STREAM);
  }

  public static InputStream castUnicodeStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR, INTEGER, DOUBLE, NUMERIC, BOOLEAN)) {
      var unicodeBytes = value.getBytes(StandardCharsets.UTF_16BE); // Encode as UTF-16BE
      return new ByteArrayInputStream(unicodeBytes);
    }
    throw castError(value, columnIndex, sourceJdbcType, UNICODE_STREAM);
  }

  public static InputStream castBinaryStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == BLOB) {
      try {
        var bytes = Base64.getDecoder().decode(value);
        return new ByteArrayInputStream(bytes);
      } catch (IllegalArgumentException e) {
        throw badB64(columnIndex, value, e);
      }
    } else if (anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR, INTEGER, DOUBLE, NUMERIC, BOOLEAN)) {
      return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)); // Encode as UTF-8
    }
    throw castError(value, columnIndex, sourceJdbcType, BINARY_STREAM);
  }

  public static Reader castCharacterStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR, INTEGER, DOUBLE, NUMERIC, BOOLEAN)) {
      return new StringReader(value);
    }
    throw castError(value, columnIndex, sourceJdbcType, CHARACTER_STREAM);
  }

  public static byte[] castBlob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (sourceJdbcType == BLOB) {
      try {
        return Base64.getDecoder().decode(value);
      } catch (IllegalArgumentException e) {
        throw badB64(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, BLOB);
  }

  public static Clob castClob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR)) {
      return new SerialClob(value.toCharArray());
    }
    throw castError(value, columnIndex, sourceJdbcType, CLOB);
  }

  public static Date castDate(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, DATE, TIMESTAMP)) {
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
        throw badDate(columnIndex, value, e);
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Date(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw badTimestamp(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, DATE);
  }

  public static Time castTime(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, TIME, TIMESTAMP)) {
      try {
        var localTime = LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
        var calendar = cal != null ? cal : Calendar.getInstance();
        var ldt = localTime.atDate(LocalDate.ofEpochDay(0)); // Epoch day for Time
        var zdt = ldt.atZone(calendar.getTimeZone().toZoneId());
        return new Time(zdt.toInstant().toEpochMilli());
      } catch (DateTimeParseException e) {
        throw badTime(columnIndex, value, e);
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Time(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw badTimestamp(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, TIME);
  }

  public static Timestamp castTimestamp(String value, int columnIndex, int sourceJdbcType, Calendar cal) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, TIMESTAMP, DATE)) {
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
        throw badTimestamp(columnIndex, value, e);
      }
    } else if (sourceJdbcType == INTEGER) {
      try {
        var seconds = Long.parseLong(value);
        return new Timestamp(seconds * 1000L); // Unix timestamp
      } catch (NumberFormatException e) {
        throw badTimestamp(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, Types.TIMESTAMP);
  }

  public static URL castURL(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, DATALINK)) {
      try {
        return new URL(value);
      } catch (MalformedURLException e) {
        throw badUrl(columnIndex, value, e);
      }
    }
    throw castError(value, columnIndex, sourceJdbcType, DATALINK);
  }

  public static NClob castNClob(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, NCLOB, NVARCHAR, CLOB)) {
      return new L4NClob(value.toCharArray());
    }
    throw castError(value, columnIndex, sourceJdbcType, NCLOB);
  }

  public static Reader castNCharacterStream(String value, int columnIndex, int sourceJdbcType) throws SQLException {
    if (anyOf(sourceJdbcType, VARCHAR, NVARCHAR, CLOB, NCLOB)) {
      return new StringReader(value);
    }
    throw castError(value, columnIndex, sourceJdbcType, NCHARACTER_STREAM);
  }

  public static <T> T castObject(String value, int columnIndex, int sourceJdbcType, Class<T> type) throws SQLException {
    if (type == null) {
      throw badType(columnIndex, value);
    }
    Object result;
    if (type == String.class && anyOf(sourceJdbcType, VARCHAR, CLOB, NCLOB, NVARCHAR)) {
      result = value;
    } else if (type == Integer.class && anyOf(sourceJdbcType, INTEGER, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      result = castInteger(value, columnIndex, sourceJdbcType);
    } else if (type == Long.class && anyOf(sourceJdbcType, INTEGER, BIGINT, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      result = castLong(value, columnIndex, sourceJdbcType);
    } else if (type == Float.class && anyOf(sourceJdbcType, FLOAT, DOUBLE, NUMERIC)) {
      result = castFloat(value, columnIndex, sourceJdbcType);
    } else if (type == Double.class && anyOf(sourceJdbcType, FLOAT, DOUBLE, NUMERIC)) {
      result = castDouble(value, columnIndex, sourceJdbcType);
    } else if (type == Byte.class && anyOf(sourceJdbcType, INTEGER, TINYINT, BOOLEAN, NUMERIC)) {
      result = castByte(value, columnIndex, sourceJdbcType);
    } else if (type == Short.class && anyOf(sourceJdbcType, INTEGER, TINYINT, SMALLINT, BOOLEAN, NUMERIC)) {
      result = castShort(value, columnIndex, sourceJdbcType);
    } else if (type == BigDecimal.class && anyOf(sourceJdbcType, INTEGER, FLOAT, DOUBLE, VARCHAR, NUMERIC, BOOLEAN, TINYINT, SMALLINT, BIGINT)) {
      result = castBigDecimal(value, columnIndex, sourceJdbcType, -1);
    } else if (type == byte[].class && sourceJdbcType == BLOB) {
      result = castBlob(value, columnIndex, sourceJdbcType);
    } else {
      throw badConversion(columnIndex, sourceJdbcType, type);
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
          throw notSupported(format("JDBC type %d for column %d", targetJdbcType, columnIndex));
      }
    } catch (Exception e) {
      throw badConversion(columnIndex, value, e);
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

  public static boolean isSelect(String rawSql) {
    return rawSql.toUpperCase().contains("SELECT");
  }

  public static L4Statement[] split(String rawSql) { // TODO maaan this is dangerous... is there a better way to do this?
    var txa = rawSql.split(";");
    return Arrays.stream(txa)
      .map(raw -> new L4Statement().sql(raw))
      .toArray(L4Statement[]::new);
  }

}
