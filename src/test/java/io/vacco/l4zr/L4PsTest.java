package io.vacco.l4zr;

import io.vacco.l4zr.jdbc.*;
import io.vacco.l4zr.rqlite.*;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import java.awt.GraphicsEnvironment;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import static io.vacco.l4zr.jdbc.L4Err.*;
import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4PsTest {

  // Helper to read InputStream content
  private static String readStream(InputStream is, String charset) throws Exception {
    if (is == null) return null;
    try (var baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = is.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      return baos.toString(charset);
    }
  }

  // Helper to read Reader content
  private static String readReader(Reader reader) throws Exception {
    if (reader == null) return null;
    try (var sw = new StringWriter()) {
      char[] buffer = new char[1024];
      int len;
      while ((len = reader.read(buffer)) != -1) {
        sw.write(buffer, 0, len);
      }
      return sw.toString();
    }
  }

  // Helper to set up test table
  private static void setupTestTable(L4Client rq) throws Exception {
    var dr = rq.executeSingle("DROP TABLE IF EXISTS ps_test_data");
    assertEquals(200, dr.statusCode);

    var createTable = String.join("\n", "",
      "CREATE TABLE ps_test_data (",
      "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
      "  num_val NUMERIC,",
      "  bool_val BOOLEAN,",
      "  tiny_val TINYINT,",
      "  small_val SMALLINT,",
      "  int_val INTEGER,",
      "  big_val BIGINT,",
      "  float_val FLOAT,",
      "  double_val DOUBLE,",
      "  text_val VARCHAR,",
      "  date_val DATE,",
      "  time_val TIME,",
      "  ts_val TIMESTAMP,",
      "  url_val DATALINK,",
      "  clob_val CLOB,",
      "  nclob_val NCLOB,",
      "  nstring_val NVARCHAR,",
      "  blob_val BLOB",
      ")"
    );
    var res0 = rq.executeSingle(createTable);
    assertEquals(200, res0.statusCode);
  }

  static {
    if (!GraphicsEnvironment.isHeadless()) {
      it("Tests L4Ps query execution and parameter setting", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (" +
          "num_val, bool_val, tiny_val, small_val, int_val, big_val, float_val, double_val, " +
          "text_val, date_val, time_val, ts_val, url_val, clob_val, nclob_val, nstring_val, blob_val" +
          ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Set parameters
        var blobData = "Hello, rqlite!".getBytes(StandardCharsets.UTF_8);
        var utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ps.setBigDecimal(1, new BigDecimal("123.45")); // num_val
        ps.setBoolean(2, true); // bool_val
        ps.setByte(3, (byte) 127); // tiny_val
        ps.setShort(4, (short) 32767); // small_val
        ps.setInt(5, 2147483647); // int_val
        ps.setLong(6, 9223372036854775807L); // big_val
        ps.setFloat(7, 3.14f); // float_val
        ps.setDouble(8, 2.71828); // double_val
        ps.setString(9, "Hello, world!"); // text_val
        ps.setDate(10, Date.valueOf("2023-10-15")); // date_val
        ps.setTime(11, Time.valueOf("14:30:00")); // time_val
        ps.setTimestamp(12, Timestamp.valueOf("2023-10-15 14:30:00")); // ts_val
        ps.setURL(13, new URL("https://example.com")); // url_val
        ps.setString(14, "This is a CLOB"); // clob_val
        ps.setNString(15, "This is an NCLOB"); // nclob_val
        ps.setNString(16, "This is an NSTRING"); // nstring_val
        ps.setBytes(17, blobData); // blob_val

        // Execute update
        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Query and verify
        var selectPs = new L4Ps(rq, "SELECT * FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals(new BigDecimal("123.45"), rs.getBigDecimal("num_val"));
        assertTrue(rs.getBoolean("bool_val"));
        assertEquals(127, rs.getByte("tiny_val"));
        assertEquals(32767, rs.getShort("small_val"));
        assertEquals(2147483647, rs.getInt("int_val"));
        assertEquals(9223372036854775807L, rs.getLong("big_val"));
        assertEquals(3.14f, rs.getFloat("float_val"), 0.001f);
        assertEquals(2.71828, rs.getDouble("double_val"), 0.001);
        assertEquals("Hello, world!", rs.getString("text_val"));
        assertEquals(Date.valueOf("2023-10-14").toString(), rs.getDate("date_val", utcCalendar).toString());
        assertEquals(Time.valueOf("09:30:00"), rs.getTime("time_val", utcCalendar));
        assertEquals(Timestamp.valueOf("2023-10-15 10:30:00"), rs.getTimestamp("ts_val", utcCalendar));
        assertEquals(new URL("https://example.com"), rs.getURL("url_val"));
        assertEquals("This is a CLOB", rs.getClob("clob_val").getSubString(1, 14));
        assertEquals("This is an NCLOB", rs.getNClob("nclob_val").getSubString(1, 16));
        assertEquals("This is an NSTRING", rs.getNString("nstring_val"));
        assertArrayEquals(blobData, rs.getBytes("blob_val"));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps stream and LOB parameter setting", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (text_val, clob_val, nclob_val, nstring_val, blob_val) VALUES (?, ?, ?, ?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Set stream and LOB parameters
        var text = "Stream text";
        var clobText = "CLOB content";
        var nclobText = "NCLOB content";
        var nstringText = "NSTRING content";
        var blobData = "BLOB data".getBytes(StandardCharsets.UTF_8);

        ps.setAsciiStream(1, new ByteArrayInputStream(text.getBytes(StandardCharsets.US_ASCII)), text.length());
        ps.setClob(2, new javax.sql.rowset.serial.SerialClob(clobText.toCharArray()));
        ps.setNClob(3, new L4NClob(nclobText.toCharArray()));
        ps.setNCharacterStream(4, new StringReader(nstringText), nstringText.length());
        ps.setBinaryStream(5, new ByteArrayInputStream(blobData), blobData.length);

        // Execute update
        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT text_val, clob_val, nclob_val, nstring_val, blob_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(text, rs.getString("text_val"));
        assertEquals(clobText, rs.getClob("clob_val").getSubString(1, clobText.length()));
        assertEquals(nclobText, rs.getNClob("nclob_val").getSubString(1, nclobText.length()));
        assertEquals(nstringText, rs.getNString("nstring_val"));
        assertArrayEquals(blobData, rs.getBytes("blob_val"));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps batch execution", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (num_val, text_val) VALUES (?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Add batch entries
        ps.setBigDecimal(1, new BigDecimal("111.11"));
        ps.setString(2, "First");
        ps.addBatch();
        ps.setBigDecimal(1, new BigDecimal("222.22"));
        ps.setString(2, "Second");
        ps.addBatch();

        // Execute batch
        int[] updateCounts = ps.executeBatch();
        assertArrayEquals(new int[]{1, 1}, updateCounts);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT num_val, text_val FROM ps_test_data ORDER BY id");
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(new BigDecimal("111.11"), rs.getBigDecimal("num_val"));
        assertEquals("First", rs.getString("text_val"));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("222.22"), rs.getBigDecimal("num_val"));
        assertEquals("Second", rs.getString("text_val"));
        assertFalse(rs.next());
        rs.close();

        // Test empty batch
        ps.clearBatch();
        updateCounts = ps.executeBatch();
        assertArrayEquals(new int[0], updateCounts);

        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps metadata retrieval", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var selectSql = "SELECT * FROM ps_test_data WHERE id = ?";
        var ps = new L4Ps(rq, selectSql);

        // Test getMetaData
        ps.setInt(1, 999);
        var meta = ps.getMetaData();

        assertEquals(18, meta.getColumnCount());
        assertEquals(Types.INTEGER, meta.getColumnType(1)); // id
        assertEquals(Types.NUMERIC, meta.getColumnType(2)); // num_val
        assertEquals(Types.BOOLEAN, meta.getColumnType(3)); // bool_val
        assertEquals("id", meta.getColumnName(1));

        // Test getParameterMetaData (unsupported)
        try {
          ps.getParameterMetaData();
          fail("Expected SQLException for getParameterMetaData");
        } catch (SQLException e) {
          assertEquals(SqlStateFeatureNotSupported, e.getSQLState());
        }

        ps.close();
      });

      it("Tests L4Ps null and object parameter setting", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (num_val, text_val, blob_val) VALUES (?, ?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Set null and object parameters
        ps.setNull(1, Types.NUMERIC);
        ps.setObject(2, "Test object", Types.VARCHAR);
        ps.setObject(3, null, Types.BLOB);
        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT num_val, text_val, blob_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertNull(rs.getBigDecimal("num_val"));
        assertTrue(rs.wasNull());
        assertEquals("Test object", rs.getString("text_val"));
        assertNull(rs.getBytes("blob_val"));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps advanced stream and LOB methods", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (text_val, clob_val, nclob_val, nstring_val, blob_val) VALUES (?, ?, ?, ?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Set advanced stream and LOB parameters
        var text = "Advanced stream";
        var clobText = "Advanced CLOB";
        var nclobText = "Advanced NCLOB";
        var nstringText = "Advanced NSTRING";
        var blobData = "Advanced BLOB".getBytes(StandardCharsets.UTF_8);

        ps.setAsciiStream(1, new ByteArrayInputStream(text.getBytes(StandardCharsets.US_ASCII)));
        ps.setClob(2, new StringReader(clobText));
        ps.setNClob(3, new StringReader(nclobText));
        ps.setNCharacterStream(4, new StringReader(nstringText));
        ps.setBlob(5, new ByteArrayInputStream(blobData));

        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT text_val, clob_val, nclob_val, nstring_val, blob_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(text, rs.getString("text_val"));
        assertEquals(clobText, rs.getClob("clob_val").getSubString(1, clobText.length()));
        assertEquals(nclobText, rs.getNClob("nclob_val").getSubString(1, nclobText.length()));
        assertEquals(nstringText, rs.getNString("nstring_val"));
        assertArrayEquals(blobData, rs.getBytes("blob_val"));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps date and time with calendar", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (date_val, time_val, ts_val) VALUES (?, ?, ?)";
        var ps = new L4Ps(rq, insertSql);

        // Set date and time with calendar
        var utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        var date = Date.valueOf("2023-10-15");
        var time = Time.valueOf("14:30:00");
        var timestamp = Timestamp.valueOf("2023-10-15 14:30:00");

        ps.setDate(1, date, utcCalendar);
        ps.setTime(2, time, utcCalendar);
        ps.setTimestamp(3, timestamp, utcCalendar);

        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT date_val, time_val, ts_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(date, rs.getDate("date_val", utcCalendar));
        assertEquals(time, rs.getTime("time_val", utcCalendar));
        assertEquals(timestamp, rs.getTimestamp("ts_val", utcCalendar));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
      });

      it("Tests L4Ps batch with error", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var insertSql = "INSERT INTO ps_test_data (num_val) VALUES (?)";
        var ps = new L4Ps(rq, insertSql);

        // Add valid batch entry
        ps.setBigDecimal(1, new BigDecimal("333.33"));
        ps.addBatch();

        // Add invalid batch entry (wrong table)
        var invalidPs = new L4Ps(rq, "INSERT INTO nonexistent_table (num_val) VALUES (?)");
        invalidPs.setBigDecimal(1, new BigDecimal("444.44"));
        invalidPs.addBatch();

        // Execute batch with error
        try {
          invalidPs.executeBatch();
          fail("Expected BatchUpdateException");
        } catch (BatchUpdateException e) {
          assertEquals(SqlStateConnectionError, e.getSQLState());
          assertArrayEquals(new int[]{}, e.getUpdateCounts());
        }

        // Verify valid batch
        int[] updateCounts = ps.executeBatch();
        assertArrayEquals(new int[]{1}, updateCounts);

        var selectPs = new L4Ps(rq, "SELECT num_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertEquals(new BigDecimal("333.33"), rs.getBigDecimal("num_val"));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        invalidPs.close();
        selectPs.close();
      });

      it("Tests L4Ps closeOnCompletion and inherited methods", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var selectSql = "SELECT * FROM ps_test_data WHERE id = ?";
        var ps = new L4Ps(rq, selectSql);

        // Test closeOnCompletion
        assertFalse(ps.isCloseOnCompletion());
        ps.closeOnCompletion();
        assertTrue(ps.isCloseOnCompletion());
        ps.setInt(1, 999); // Non-existent ID
        var rs = ps.executeQuery();
        assertTrue(ps.isClosed());

        // Test inherited methods
        ps = new L4Ps(rq, selectSql);
        ps.setMaxRows(1);
        assertEquals(1, ps.getMaxRows());
        ps.setQueryTimeout(10);
        assertEquals(10, ps.getQueryTimeout());
        ps.setFetchSize(10);
        assertEquals(10, ps.getFetchSize());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, ps.getResultSetType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, ps.getResultSetConcurrency());

        ps.close();
      });

      it("Tests L4Ps unsupported operations and error handling", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);
        var selectSql = "SELECT * FROM ps_test_data WHERE id = ?";
        var ps = new L4Ps(rq, selectSql);

        // Test unsupported parameter types
        try {
          ps.setRef(1, null);
          fail("Expected SQLException for setRef");
        } catch (SQLException e) {
          assertEquals(SqlStateFeatureNotSupported, e.getSQLState());
        }
        try {
          ps.setArray(1, null);
          fail("Expected SQLException for setArray");
        } catch (SQLException e) {
          assertEquals(SqlStateFeatureNotSupported, e.getSQLState());
        }
        try {
          ps.setRowId(1, null);
          fail("Expected SQLException for setRowId");
        } catch (SQLException e) {
          assertEquals(SqlStateFeatureNotSupported, e.getSQLState());
        }
        try {
          ps.setSQLXML(1, null);
          fail("Expected SQLException for setSQLXML");
        } catch (SQLException e) {
          assertEquals(SqlStateFeatureNotSupported, e.getSQLState());
        }

        // Test invalid execution methods
        try {
          ps.executeQuery("SELECT * FROM ps_test_data");
          fail("Expected SQLException for executeQuery(String)");
        } catch (SQLException e) {
          assertEquals(SqlStateInvalidQuery, e.getSQLState());
        }
        try {
          ps.executeUpdate("UPDATE ps_test_data SET num_val = 1");
          fail("Expected SQLException for executeUpdate(String)");
        } catch (SQLException e) {
          assertEquals(SqlStateInvalidQuery, e.getSQLState());
        }
        try {
          ps.execute("SELECT * FROM ps_test_data");
          fail("Expected SQLException for execute(String)");
        } catch (SQLException e) {
          assertEquals(SqlStateInvalidQuery, e.getSQLState());
        }

        // Test closed statement
        ps.close();
        try {
          ps.setInt(1, 1);
          fail("Expected SQLException for closed statement");
        } catch (SQLException e) {
          assertEquals(SqlStateGeneralError, e.getSQLState());
        }
        try {
          ps.executeQuery();
          fail("Expected SQLException for closed statement");
        } catch (SQLException e) {
          assertEquals(SqlStateGeneralError, e.getSQLState());
        }

        // Test unwrap
        ps = new L4Ps(rq, selectSql);
        assertTrue(ps.isWrapperFor(PreparedStatement.class));
        assertTrue(ps.isWrapperFor(Statement.class));
        assertTrue(ps.isWrapperFor(Wrapper.class));
        assertFalse(ps.isWrapperFor(String.class));
        assertSame(ps, ps.unwrap(PreparedStatement.class));
        try {
          ps.unwrap(String.class);
          fail("Expected SQLException for invalid unwrap");
        } catch (SQLException e) {
          assertNotNull(e.getMessage());
        }

        ps.close();
      });

      it("Tests L4Ps edge cases and invalid inputs", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());
        setupTestTable(rq);

        // Test null/empty SQL
        try {
          new L4Ps(rq, null);
          fail("Expected SQLException for null SQL");
        } catch (SQLException e) {
          assertEquals(SqlStateInvalidQuery, e.getSQLState());
        }
        try {
          new L4Ps(rq, "");
          fail("Expected SQLException for empty SQL");
        } catch (SQLException e) {
          assertEquals(SqlStateInvalidQuery, e.getSQLState());
        }

        // Test invalid SQL execution
        var invalidPs = new L4Ps(rq, "SELECT * FROM nonexistent_table WHERE id = ?");
        invalidPs.setInt(1, 1);
        try {
          invalidPs.executeQuery();
          fail("Expected SQLException for invalid table");
        } catch (SQLException e) {
          assertEquals(SqlStateConnectionError, e.getSQLState());
        }

        // Test clearParameters
        var ps = new L4Ps(rq, "INSERT INTO ps_test_data (num_val, text_val) VALUES (?, ?)");
        ps.setBigDecimal(1, new BigDecimal("123.45"));
        ps.setString(2, "Test");
        ps.clearParameters();
        ps.setNull(1, Types.NUMERIC);
        ps.setNull(2, Types.VARCHAR);
        int rowsAffected = ps.executeUpdate();
        assertEquals(1, rowsAffected);

        // Verify
        var selectPs = new L4Ps(rq, "SELECT num_val, text_val FROM ps_test_data WHERE id = ?");
        selectPs.setInt(1, 1);
        var rs = selectPs.executeQuery();
        assertTrue(rs.next());
        assertNull(rs.getBigDecimal("num_val"));
        assertNull(rs.getString("text_val"));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        selectPs.close();
        invalidPs.close();
      });
    }
  }
}
