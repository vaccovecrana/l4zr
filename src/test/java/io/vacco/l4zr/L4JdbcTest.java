package io.vacco.l4zr;

import io.vacco.l4zr.json.Json;
import io.vacco.l4zr.rqlite.L4Result;
import j8spec.UnsafeBlock;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.Date;
import io.vacco.l4zr.jdbc.L4Jdbc;

import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4JdbcTest {

  // Helper to read InputStream content
  private static String readStream(InputStream is, String charset) throws Exception {
    if (is == null) return null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = is.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      return new String(baos.toByteArray(), charset);
    }
  }

  // Helper to read Reader content
  private static String readReader(Reader reader) throws Exception {
    if (reader == null) return null;
    try (StringWriter sw = new StringWriter()) {
      char[] buffer = new char[1024];
      int len;
      while ((len = reader.read(buffer)) != -1) {
        sw.write(buffer, 0, len);
      }
      return sw.toString();
    }
  }

  // Helper to test expected SQLException
  private static void runFail(UnsafeBlock action, String expectedSqlState) {
    try {
      action.tryToExecute();
      fail("Expected SQLException with SQLState " + expectedSqlState);
    } catch (Throwable e) {
      System.out.println(e);
    }
  }

  // Helper to create a mock L4Result
  private static L4Result createMockResult(List<String> columns, List<String> types, List<List<String>> values) {
    var result = new L4Result(Json.object());
    result.columns = new ArrayList<>(columns);
    result.types = new ArrayList<>(types);
    result.values = new ArrayList<>(values);
    return result;
  }

  static {
    it("Tests L4Jdbc utility methods", () -> {
      // Mock L4Result
      var result = createMockResult(
        Arrays.asList("id", "name"),
        Arrays.asList(L4Jdbc.RQ_INTEGER, L4Jdbc.RQ_VARCHAR),
        Arrays.asList(Arrays.asList("1", "Alice"), Arrays.asList("2", "Bob"))
      );

      // Test checkColumn
      L4Jdbc.checkColumn(1, result); // Valid
      L4Jdbc.checkColumn(2, result); // Valid
      runFail(() -> L4Jdbc.checkColumn(0, result), L4Jdbc.SqlStateInvalidColumn);
      runFail(() -> L4Jdbc.checkColumn(3, result), L4Jdbc.SqlStateInvalidColumn);

      // Test checkColumnLabel
      L4Jdbc.checkColumnLabel("id", result); // Valid
      L4Jdbc.checkColumnLabel("name", result); // Valid
      runFail(() -> L4Jdbc.checkColumnLabel(null, result), L4Jdbc.SqlStateInvalidParam);
      runFail(() -> L4Jdbc.checkColumnLabel("age", result), L4Jdbc.SqlStateInvalidParam);

      // Test checkRow
      L4Jdbc.checkRow(0, result, false); // Valid
      L4Jdbc.checkRow(1, result, false); // Valid
      runFail(() -> L4Jdbc.checkRow(-1, result, false), L4Jdbc.SqlStateInvalidCursor);
      runFail(() -> L4Jdbc.checkRow(2, result, false), L4Jdbc.SqlStateInvalidCursor);
      runFail(() -> L4Jdbc.checkRow(0, result, true), L4Jdbc.SqlStateClosed);
    });

    it("Tests L4Jdbc getJdbcType", () -> {
      assertEquals(Types.INTEGER, L4Jdbc.getJdbcType(L4Jdbc.RQ_INTEGER));
      assertEquals(Types.NUMERIC, L4Jdbc.getJdbcType(L4Jdbc.RQ_NUMERIC));
      assertEquals(Types.BOOLEAN, L4Jdbc.getJdbcType(L4Jdbc.RQ_BOOLEAN));
      assertEquals(Types.TINYINT, L4Jdbc.getJdbcType(L4Jdbc.RQ_TINYINT));
      assertEquals(Types.SMALLINT, L4Jdbc.getJdbcType(L4Jdbc.RQ_SMALLINT));
      assertEquals(Types.BIGINT, L4Jdbc.getJdbcType(L4Jdbc.RQ_BIGINT));
      assertEquals(Types.FLOAT, L4Jdbc.getJdbcType(L4Jdbc.RQ_FLOAT));
      assertEquals(Types.DOUBLE, L4Jdbc.getJdbcType(L4Jdbc.RQ_DOUBLE));
      assertEquals(Types.VARCHAR, L4Jdbc.getJdbcType(L4Jdbc.RQ_VARCHAR));
      assertEquals(Types.DATE, L4Jdbc.getJdbcType(L4Jdbc.RQ_DATE));
      assertEquals(Types.TIME, L4Jdbc.getJdbcType(L4Jdbc.RQ_TIME));
      assertEquals(Types.TIMESTAMP, L4Jdbc.getJdbcType(L4Jdbc.RQ_TIMESTAMP));
      assertEquals(Types.DATALINK, L4Jdbc.getJdbcType(L4Jdbc.RQ_DATALINK));
      assertEquals(Types.CLOB, L4Jdbc.getJdbcType(L4Jdbc.RQ_CLOB));
      assertEquals(Types.NCLOB, L4Jdbc.getJdbcType(L4Jdbc.RQ_NCLOB));
      assertEquals(Types.NVARCHAR, L4Jdbc.getJdbcType(L4Jdbc.RQ_NVARCHAR));
      assertEquals(Types.BLOB, L4Jdbc.getJdbcType(L4Jdbc.RQ_BLOB));
      assertEquals(Types.NULL, L4Jdbc.getJdbcType(L4Jdbc.RQ_NULL));
      assertEquals(-1, L4Jdbc.getJdbcType("UNKNOWN"));
      try {
        L4Jdbc.getJdbcType(null);
        fail("Expected IllegalArgumentException for null type");
      } catch (IllegalArgumentException e) {
        assertNotNull(e.getMessage());
      }
      assertEquals(Types.INTEGER, L4Jdbc.getJdbcType("integer")); // Case-insensitive
    });

    it("Tests L4Jdbc primitive type conversions", () -> {
      int colIdx = 1;

      // castBoolean
      assertTrue(L4Jdbc.castBoolean("1", colIdx, Types.INTEGER));
      assertFalse(L4Jdbc.castBoolean("0", colIdx, Types.INTEGER));
      assertTrue(L4Jdbc.castBoolean("true", colIdx, Types.VARCHAR));
      assertFalse(L4Jdbc.castBoolean("false", colIdx, Types.VARCHAR));
      runFail(() -> L4Jdbc.castBoolean("2", colIdx, Types.INTEGER), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castBoolean("abc", colIdx, Types.INTEGER), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castBoolean("true", colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);

      // castInteger
      assertEquals(123, L4Jdbc.castInteger("123", colIdx, Types.INTEGER));
      assertEquals(127, L4Jdbc.castInteger("127", colIdx, Types.TINYINT));
      runFail(() -> L4Jdbc.castInteger("2147483648", colIdx, Types.INTEGER), L4Jdbc.SqlStateInvalidType); // Out of range
      runFail(() -> L4Jdbc.castInteger("abc", colIdx, Types.INTEGER), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castInteger("123", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castLong
      assertEquals(9223372036854775807L, L4Jdbc.castLong("9223372036854775807", colIdx, Types.BIGINT));
      assertEquals(123, L4Jdbc.castLong("123", colIdx, Types.INTEGER));
      runFail(() -> L4Jdbc.castLong("abc", colIdx, Types.BIGINT), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castLong("123", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castFloat
      assertEquals(3.14f, L4Jdbc.castFloat("3.14", colIdx, Types.FLOAT), 0.001f);
      assertEquals(2.718, L4Jdbc.castFloat("2.718", colIdx, Types.DOUBLE), 0.001f);
      runFail(() -> L4Jdbc.castFloat("abc", colIdx, Types.FLOAT), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castFloat("3.14", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castDouble
      assertEquals(3.14, L4Jdbc.castDouble("3.14", colIdx, Types.DOUBLE), 0.001);
      assertEquals(2.718, L4Jdbc.castDouble("2.718", colIdx, Types.FLOAT), 0.001);
      runFail(() -> L4Jdbc.castDouble("abc", colIdx, Types.DOUBLE), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castDouble("3.14", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castByte
      assertEquals(127, L4Jdbc.castByte("127", colIdx, Types.TINYINT));
      assertEquals(1, L4Jdbc.castByte("1", colIdx, Types.BOOLEAN));
      runFail(() -> L4Jdbc.castByte("128", colIdx, Types.TINYINT), L4Jdbc.SqlStateInvalidType); // Out of range
      runFail(() -> L4Jdbc.castByte("abc", colIdx, Types.TINYINT), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castByte("127", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castShort
      assertEquals(32767, L4Jdbc.castShort("32767", colIdx, Types.SMALLINT));
      assertEquals(127, L4Jdbc.castShort("127", colIdx, Types.TINYINT));
      runFail(() -> L4Jdbc.castShort("32768", colIdx, Types.SMALLINT), L4Jdbc.SqlStateInvalidType); // Out of range
      runFail(() -> L4Jdbc.castShort("abc", colIdx, Types.SMALLINT), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castShort("32767", colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc BigDecimal conversion", () -> {
      int colIdx = 1;
      assertEquals(new BigDecimal("123.45"), L4Jdbc.castBigDecimal("123.45", colIdx, Types.NUMERIC, -1));
      assertEquals(new BigDecimal("123"), L4Jdbc.castBigDecimal("123.45", colIdx, Types.NUMERIC, 0));
      assertEquals(new BigDecimal("123.4500"), L4Jdbc.castBigDecimal("123.45", colIdx, Types.NUMERIC, 4));
      assertEquals(new BigDecimal("123"), L4Jdbc.castBigDecimal("123", colIdx, Types.INTEGER, -1));
      assertEquals(new BigDecimal("3.14"), L4Jdbc.castBigDecimal("3.14", colIdx, Types.FLOAT, -1));
      runFail(() -> L4Jdbc.castBigDecimal("abc", colIdx, Types.NUMERIC, -1), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castBigDecimal("123.45", colIdx, Types.BLOB, -1), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc stream conversions", () -> {
      int colIdx = 1;
      String value = "Hello, world!";

      // castAsciiStream
      assertEquals(value, readStream(L4Jdbc.castAsciiStream(value, colIdx, Types.VARCHAR), StandardCharsets.US_ASCII.toString()));
      assertEquals(value, readStream(L4Jdbc.castAsciiStream(value, colIdx, Types.CLOB), StandardCharsets.US_ASCII.toString()));
      assertEquals("123", readStream(L4Jdbc.castAsciiStream("123", colIdx, Types.INTEGER), StandardCharsets.US_ASCII.toString()));
      runFail(() -> L4Jdbc.castAsciiStream(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);

      // castUnicodeStream
      assertEquals(value, readStream(L4Jdbc.castUnicodeStream(value, colIdx, Types.VARCHAR), StandardCharsets.UTF_16BE.toString()));
      assertEquals(value, readStream(L4Jdbc.castUnicodeStream(value, colIdx, Types.NCLOB), StandardCharsets.UTF_16BE.toString()));
      assertEquals("3.14", readStream(L4Jdbc.castUnicodeStream("3.14", colIdx, Types.DOUBLE), StandardCharsets.UTF_16BE.toString()));
      runFail(() -> L4Jdbc.castUnicodeStream(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);

      // castBinaryStream
      assertEquals(value, readStream(L4Jdbc.castBinaryStream(value, colIdx, Types.VARCHAR), StandardCharsets.UTF_8.toString()));
      assertEquals(value, readStream(L4Jdbc.castBinaryStream(value, colIdx, Types.CLOB), StandardCharsets.UTF_8.toString()));
      String base64Blob = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
      assertEquals(value, readStream(L4Jdbc.castBinaryStream(base64Blob, colIdx, Types.BLOB), StandardCharsets.UTF_8.toString()));
      runFail(() -> L4Jdbc.castBinaryStream("invalid-base64", colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castBinaryStream(value, colIdx, Types.DATE), L4Jdbc.SqlStateInvalidConversion);

      // castCharacterStream
      assertEquals(value, readReader(L4Jdbc.castCharacterStream(value, colIdx, Types.VARCHAR)));
      assertEquals(value, readReader(L4Jdbc.castCharacterStream(value, colIdx, Types.NCLOB)));
      assertEquals("true", readReader(L4Jdbc.castCharacterStream("true", colIdx, Types.BOOLEAN)));
      runFail(() -> L4Jdbc.castCharacterStream(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);

      // castNCharacterStream
      assertEquals(value, readReader(L4Jdbc.castNCharacterStream(value, colIdx, Types.NVARCHAR)));
      assertEquals(value, readReader(L4Jdbc.castNCharacterStream(value, colIdx, Types.CLOB)));
      runFail(() -> L4Jdbc.castNCharacterStream(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc LOB conversions", () -> {
      int colIdx = 1;
      String value = "Hello, world!";
      String base64Blob = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));

      // castBlob
      assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), L4Jdbc.castBlob(base64Blob, colIdx, Types.BLOB));
      runFail(() -> L4Jdbc.castBlob("invalid-base64", colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castBlob(value, colIdx, Types.VARCHAR), L4Jdbc.SqlStateInvalidConversion);

      // castClob
      Clob clob = L4Jdbc.castClob(value, colIdx, Types.CLOB);
      assertEquals(value, clob.getSubString(1, value.length()));
      clob = L4Jdbc.castClob(value, colIdx, Types.VARCHAR);
      assertEquals(value, clob.getSubString(1, value.length()));
      runFail(() -> L4Jdbc.castClob(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);

      // castNClob
      NClob nclob = L4Jdbc.castNClob(value, colIdx, Types.NCLOB);
      assertEquals(value, nclob.getSubString(1, value.length()));
      nclob = L4Jdbc.castNClob(value, colIdx, Types.NVARCHAR);
      assertEquals(value, nclob.getSubString(1, value.length()));
      runFail(() -> L4Jdbc.castNClob(value, colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc date and time conversions", () -> {
      int colIdx = 1;
      Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      // castDate
      Date expectedDate = new Date(LocalDate.of(2023, 10, 15).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli());
      assertEquals(expectedDate, L4Jdbc.castDate("2023-10-15", colIdx, Types.DATE, utcCal));
      assertEquals(expectedDate, L4Jdbc.castDate("2023-10-15T00:00:00Z", colIdx, Types.TIMESTAMP, utcCal));
      assertEquals(new Date(1697328000000L), L4Jdbc.castDate("1697328000", colIdx, Types.INTEGER, utcCal)); // Unix timestamp
      runFail(() -> L4Jdbc.castDate("invalid-date", colIdx, Types.DATE, utcCal), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castDate("2023-10-15", colIdx, Types.BLOB, utcCal), L4Jdbc.SqlStateInvalidConversion);

      // castTime
      Time expectedTime = Time.valueOf(LocalTime.of(14, 30));
      assertEquals(expectedTime, L4Jdbc.castTime("14:30:00", colIdx, Types.TIME, null));
      assertEquals(expectedTime, L4Jdbc.castTime("14:30:00", colIdx, Types.VARCHAR, null));
      assertEquals(new Time(3600000L), L4Jdbc.castTime("3600", colIdx, Types.INTEGER, null)); // Unix timestamp
      runFail(() -> L4Jdbc.castTime("invalid-time", colIdx, Types.TIME, null), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castTime("14:30:00", colIdx, Types.BLOB, null), L4Jdbc.SqlStateInvalidConversion);

      // castTimestamp
      Timestamp expectedTs = new Timestamp(
        LocalDateTime.of(2023, 10, 15, 14, 30).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
      );
      assertEquals(expectedTs, L4Jdbc.castTimestamp("2023-10-15 14:30:00", colIdx, Types.TIMESTAMP, utcCal));
      assertEquals(expectedTs, L4Jdbc.castTimestamp("2023-10-15T14:30:00Z", colIdx, Types.VARCHAR, utcCal));
      assertEquals(new Timestamp(1697328000000L), L4Jdbc.castTimestamp("1697328000", colIdx, Types.INTEGER, utcCal));
      runFail(() -> L4Jdbc.castTimestamp("invalid-ts", colIdx, Types.TIMESTAMP, utcCal), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castTimestamp("2023-10-15 14:30:00", colIdx, Types.BLOB, utcCal), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc URL conversion", () -> {
      int colIdx = 1;
      URL expectedUrl = new URL("https://example.com");
      assertEquals(expectedUrl, L4Jdbc.castURL("https://example.com", colIdx, Types.DATALINK));
      assertEquals(expectedUrl, L4Jdbc.castURL("https://example.com", colIdx, Types.VARCHAR));
      runFail(() -> L4Jdbc.castURL("invalid-url", colIdx, Types.DATALINK), L4Jdbc.SqlStateInvalidType);
      runFail(() -> L4Jdbc.castURL("https://example.com", colIdx, Types.BLOB), L4Jdbc.SqlStateInvalidConversion);
    });

    it("Tests L4Jdbc object conversion", () -> {
      int colIdx = 1;

      // String
      assertEquals("Hello", L4Jdbc.castObject("Hello", colIdx, Types.VARCHAR, String.class));
      assertEquals("123", L4Jdbc.castObject("123", colIdx, Types.CLOB, String.class));
      runFail(() -> L4Jdbc.castObject("Hello", colIdx, Types.BLOB, String.class), L4Jdbc.SqlStateFeatureNotSupported);

      // Integer
      assertEquals(123, (int) L4Jdbc.castObject("123", colIdx, Types.INTEGER, Integer.class));
      assertEquals(127, (int) L4Jdbc.castObject("127", colIdx, Types.TINYINT, Integer.class));
      runFail(() -> L4Jdbc.castObject("abc", colIdx, Types.INTEGER, Integer.class), L4Jdbc.SqlStateInvalidType);

      // Long
      assertEquals(123L, (long) L4Jdbc.castObject("123", colIdx, Types.BIGINT, Long.class));
      runFail(() -> L4Jdbc.castObject("abc", colIdx, Types.BIGINT, Long.class), L4Jdbc.SqlStateInvalidType);

      // Float
      assertEquals(3.14f, L4Jdbc.castObject("3.14", colIdx, Types.FLOAT, Float.class), 0.001f);
      runFail(() -> L4Jdbc.castObject("abc", colIdx, Types.FLOAT, Float.class), L4Jdbc.SqlStateInvalidType);

      // Double
      assertEquals(3.14, L4Jdbc.castObject("3.14", colIdx, Types.DOUBLE, Double.class), 0.001);
      runFail(() -> L4Jdbc.castObject("abc", colIdx, Types.DOUBLE, Double.class), L4Jdbc.SqlStateInvalidType);

      // Byte
      assertEquals((byte) 127, (byte) L4Jdbc.castObject("127", colIdx, Types.TINYINT, Byte.class));
      runFail(() -> L4Jdbc.castObject("128", colIdx, Types.TINYINT, Byte.class), L4Jdbc.SqlStateInvalidType);

      // Short
      assertEquals((short) 32767, (short) L4Jdbc.castObject("32767", colIdx, Types.SMALLINT, Short.class));
      runFail(() -> L4Jdbc.castObject("32768", colIdx, Types.SMALLINT, Short.class), L4Jdbc.SqlStateInvalidType);

      // BigDecimal
      assertEquals(new BigDecimal("123.45"), L4Jdbc.castObject("123.45", colIdx, Types.NUMERIC, BigDecimal.class));
      runFail(() -> L4Jdbc.castObject("abc", colIdx, Types.NUMERIC, BigDecimal.class), L4Jdbc.SqlStateInvalidType);

      // byte[]
      byte[] bytes = "Hello".getBytes(StandardCharsets.UTF_8);
      String base64 = Base64.getEncoder().encodeToString(bytes);
      assertArrayEquals(bytes, L4Jdbc.castObject(base64, colIdx, Types.BLOB, byte[].class));
      runFail(() -> L4Jdbc.castObject("invalid-base64", colIdx, Types.BLOB, byte[].class), L4Jdbc.SqlStateInvalidType);

      // Unsupported type
      runFail(() -> L4Jdbc.castObject("123", colIdx, Types.INTEGER, URL.class), L4Jdbc.SqlStateFeatureNotSupported);
      runFail(() -> L4Jdbc.castObject("123", colIdx, Types.INTEGER, null), L4Jdbc.SqlStateInvalidType);
    });

    it("Tests L4Jdbc convertValue", () -> {
      int colIdx = 1;
      Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      String value = "Hello, world!";
      String base64Blob = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));

      // VARCHAR
      assertEquals("123", L4Jdbc.convertValue("123", Types.VARCHAR, Types.VARCHAR, colIdx, -1, null, null));

      // BOOLEAN
      assertTrue((Boolean) L4Jdbc.convertValue("1", Types.INTEGER, Types.BOOLEAN, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("2", Types.INTEGER, Types.BOOLEAN, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // INTEGER
      assertEquals(123, L4Jdbc.convertValue("123", Types.INTEGER, Types.INTEGER, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("abc", Types.INTEGER, Types.INTEGER, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // BIGINT
      assertEquals(123L, L4Jdbc.convertValue("123", Types.BIGINT, Types.BIGINT, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("abc", Types.BIGINT, Types.BIGINT, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // DOUBLE
      assertEquals(3.14, L4Jdbc.convertValue("3.14", Types.DOUBLE, Types.DOUBLE, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("abc", Types.DOUBLE, Types.DOUBLE, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // FLOAT
      assertEquals(3.14f, L4Jdbc.convertValue("3.14", Types.FLOAT, Types.FLOAT, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("abc", Types.FLOAT, Types.FLOAT, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // TINYINT
      assertEquals((byte) 127, L4Jdbc.convertValue("127", Types.TINYINT, Types.TINYINT, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("128", Types.TINYINT, Types.TINYINT, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // SMALLINT
      assertEquals((short) 32767, L4Jdbc.convertValue("32767", Types.SMALLINT, Types.SMALLINT, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("32768", Types.SMALLINT, Types.SMALLINT, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // DECIMAL
      assertEquals(new BigDecimal("123.45"), L4Jdbc.convertValue("123.45", Types.NUMERIC, Types.DECIMAL, colIdx, -1, null, null));
      assertEquals(new BigDecimal("123"), L4Jdbc.convertValue("123.45", Types.NUMERIC, Types.DECIMAL, colIdx, 0, null, null));
      runFail(() -> L4Jdbc.convertValue("abc", Types.NUMERIC, Types.DECIMAL, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // BLOB
      assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), (byte[]) L4Jdbc.convertValue(base64Blob, Types.BLOB, Types.BLOB, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("invalid-base64", Types.BLOB, Types.BLOB, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // DATE
      Date expectedDate = new Date(LocalDate.of(2023, 10, 15).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli());
      assertEquals(expectedDate, L4Jdbc.convertValue("2023-10-15", Types.DATE, Types.DATE, colIdx, -1, utcCal, null));
      runFail(() -> L4Jdbc.convertValue("invalid-date", Types.DATE, Types.DATE, colIdx, -1, utcCal, null), L4Jdbc.SqlStateInvalidType);

      // TIME
      Time expectedTime = Time.valueOf(LocalTime.of(14, 30));
      assertEquals(expectedTime, L4Jdbc.convertValue("14:30:00", Types.TIME, Types.TIME, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("invalid-time", Types.TIME, Types.TIME, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // TIMESTAMP
      Timestamp expectedTs = new Timestamp(
        LocalDateTime.of(2023, 10, 15, 14, 30).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
      );
      assertEquals(expectedTs, L4Jdbc.convertValue("2023-10-15 14:30:00", Types.TIMESTAMP, Types.TIMESTAMP, colIdx, -1, utcCal, null));
      runFail(() -> L4Jdbc.convertValue("invalid-ts", Types.TIMESTAMP, Types.TIMESTAMP, colIdx, -1, utcCal, null), L4Jdbc.SqlStateInvalidType);

      // VARCHAR_STREAM
      assertEquals(value, readStream((InputStream) L4Jdbc.convertValue(value, Types.VARCHAR, L4Jdbc.VARCHAR_STREAM, colIdx, -1, null, null), StandardCharsets.US_ASCII.toString()));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.VARCHAR_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // UNICODE_STREAM
      assertEquals(value, readStream((InputStream) L4Jdbc.convertValue(value, Types.VARCHAR, L4Jdbc.UNICODE_STREAM, colIdx, -1, null, null), StandardCharsets.UTF_16BE.toString()));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.UNICODE_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // BINARY_STREAM
      assertEquals(value, readStream((InputStream) L4Jdbc.convertValue(base64Blob, Types.BLOB, L4Jdbc.BINARY_STREAM, colIdx, -1, null, null), StandardCharsets.UTF_8.toString()));
      runFail(() -> L4Jdbc.convertValue("invalid-base64", Types.BLOB, L4Jdbc.BINARY_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // CHARACTER_STREAM
      assertEquals(value, readReader((Reader) L4Jdbc.convertValue(value, Types.VARCHAR, L4Jdbc.CHARACTER_STREAM, colIdx, -1, null, null)));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.CHARACTER_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // CLOB_STREAM
      Clob clob = (Clob) L4Jdbc.convertValue(value, Types.CLOB, L4Jdbc.CLOB_STREAM, colIdx, -1, null, null);
      assertEquals(value, clob.getSubString(1, value.length()));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.CLOB_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // NCLOB_STREAM
      NClob nclob = (NClob) L4Jdbc.convertValue(value, Types.NCLOB, L4Jdbc.NCLOB_STREAM, colIdx, -1, null, null);
      assertEquals(value, nclob.getSubString(1, value.length()));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.NCLOB_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // NCHARACTER_STREAM
      assertEquals(value, readReader((Reader) L4Jdbc.convertValue(value, Types.NVARCHAR, L4Jdbc.NCHARACTER_STREAM, colIdx, -1, null, null)));
      runFail(() -> L4Jdbc.convertValue(value, Types.BLOB, L4Jdbc.NCHARACTER_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidConversion);

      // URL_STREAM
      URL expectedUrl = new URL("https://example.com");
      assertEquals(expectedUrl, L4Jdbc.convertValue("https://example.com", Types.DATALINK, L4Jdbc.URL_STREAM, colIdx, -1, null, null));
      runFail(() -> L4Jdbc.convertValue("invalid-url", Types.DATALINK, L4Jdbc.URL_STREAM, colIdx, -1, null, null), L4Jdbc.SqlStateInvalidType);

      // OBJECT_STREAM
      assertEquals(123, L4Jdbc.convertValue("123", Types.INTEGER, L4Jdbc.OBJECT_STREAM, colIdx, -1, null, Integer.class));
      assertEquals(value, L4Jdbc.convertValue(value, Types.VARCHAR, L4Jdbc.OBJECT_STREAM, colIdx, -1, null, String.class));
      runFail(() -> L4Jdbc.convertValue("123", Types.INTEGER, L4Jdbc.OBJECT_STREAM, colIdx, -1, null, URL.class), L4Jdbc.SqlStateFeatureNotSupported);

      // NULL
      assertNull(L4Jdbc.convertValue("null", Types.NULL, Types.NULL, colIdx, -1, null, null));

      // Unsupported target type
      runFail(() -> L4Jdbc.convertValue("123", Types.INTEGER, Types.ARRAY, colIdx, -1, null, null), L4Jdbc.SqlStateFeatureNotSupported);
    });
  }
}