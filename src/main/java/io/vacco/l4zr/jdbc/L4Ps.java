package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Objects;

import static io.vacco.l4zr.jdbc.L4Jdbc.*;

public class L4Ps implements PreparedStatement {

  private final L4Client client;
  private final L4Statement statement;
  private boolean isClosed = false;
  private ResultSet currentResultSet = null;
  private int maxRows = 0; // For limiting result set rows (optional)
  private int fetchSize = 0; // For fetch size hint (optional)

  public L4Ps(L4Client client, String sql) {
    this.client = Objects.requireNonNull(client);
    this.statement = new L4Statement().sql(Objects.requireNonNull(sql));
  }

  private void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("PreparedStatement is closed", L4Jdbc.SqlStateClosed);
    }
  }

  private void closeCurrentResultSet() throws SQLException {
    if (currentResultSet != null && !currentResultSet.isClosed()) {
      currentResultSet.close();
    }
    currentResultSet = null;
  }

  private void tryRun(L4Block b) throws SQLException {
    try {
      b.tryRun();
    } catch (IllegalArgumentException e) {
      throw new SQLException(e.getMessage(), SqlStateInvalidParam, e);
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), SqlStateInvalidAttr, e);
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate() throws SQLException {
    return 0;
  }

  @Override public void setNull(int parameterIndex, int sqlType) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex - 1, null));
  }

  @Override public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setByte(int parameterIndex, byte x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setShort(int parameterIndex, short x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setInt(int parameterIndex, int x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex - 1, x));
  }

  @Override public void setLong(int parameterIndex, long x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex - 1, x));
  }

  @Override public void setFloat(int parameterIndex, float x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setDouble(int parameterIndex, double x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex -1, x));
  }

  @Override public void setString(int parameterIndex, String x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex - 1, x));
  }

  @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    checkClosed();
    tryRun(() -> statement.withPositionalParam(parameterIndex - 1, x));
  }

  @Override public void setDate(int parameterIndex, Date x) throws SQLException {
    checkClosed();
    tryRun(() -> {
      var dateStr = x == null ? null : new SimpleDateFormat("yyyy-MM-dd").format(x);
      statement.withPositionalParam(parameterIndex - 1, dateStr);
    });
  }

  @Override public void setTime(int parameterIndex, Time x) throws SQLException {
    checkClosed();
    tryRun(() -> {
        var timeStr = x == null ? null : new SimpleDateFormat("HH:mm:ss").format(x);
        statement.withPositionalParam(parameterIndex - 1, timeStr);
    });
  }

  @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    checkClosed();
    tryRun(() -> {
      var timestampStr = x == null ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(x);
      statement.withPositionalParam(parameterIndex - 1, timestampStr);
    });
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void clearParameters() throws SQLException {

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {

  }

  @Override
  public boolean execute() throws SQLException {
    return false;
  }

  @Override
  public void addBatch() throws SQLException {

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {

  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {

  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {

  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    checkClosed();
    tryRun(() -> {
      var dateStr = (String) null;
      if (x != null) {
        var sdf = new SimpleDateFormat("yyyy-MM-dd");
        if (cal != null) {
          sdf.setTimeZone(cal.getTimeZone());
        }
        dateStr = sdf.format(x);
      }
      statement.withPositionalParam(parameterIndex - 1, dateStr);
    });
  }

  @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    checkClosed();
    tryRun(() -> {
      var timeStr = (String) null;
      if (x != null) {
        var sdf = new SimpleDateFormat("HH:mm:ss");
        if (cal != null) {
          sdf.setTimeZone(cal.getTimeZone());
        }
        timeStr = sdf.format(x);
      }
      statement.withPositionalParam(parameterIndex - 1, timeStr);
    });
  }

  @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    checkClosed();
    tryRun(() -> {
      var timestampStr = (String) null;
      if (x != null) {
        var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        if (cal != null) {
          sdf.setTimeZone(cal.getTimeZone());
        }
        timestampStr = sdf.format(x);
      }
      statement.withPositionalParam(parameterIndex - 1, timestampStr);
    });
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {

  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {

  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {

  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return 0;
  }

  @Override public void close() throws SQLException {
    if (!isClosed) {
      closeCurrentResultSet();
      isClosed = true;
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {

  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {

  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {

  }

  @Override
  public void cancel() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String name) throws SQLException {

  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {

  }

  @Override
  public void clearBatch() throws SQLException {

  }

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {

  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException("Interface cannot be null");
    }
    if (iface == PreparedStatement.class || iface == Wrapper.class) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException("Interface cannot be null");
    }
    return iface == PreparedStatement.class || iface == Wrapper.class;
  }

}
