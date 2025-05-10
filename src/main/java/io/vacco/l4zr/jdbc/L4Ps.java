package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static io.vacco.l4zr.jdbc.L4Err.badAttr;
import static io.vacco.l4zr.jdbc.L4Err.badParam;

public class L4Ps extends L4St implements PreparedStatement {

  private final L4Statement statement;
  private boolean isClosed = false;
  private ResultSet currentResultSet = null;
  private int maxRows = 0; // For limiting result set rows (optional)
  private int fetchSize = 0; // For fetch size hint (optional)

  public L4Ps(L4Client client) {
    super(client);
    this.statement = null;
  }

  public void tryRun(L4Block b) throws SQLException {
    try {
      b.tryRun();
    } catch (IllegalArgumentException e) {
      throw badParam(e);
    } catch (Exception e) {
      throw badAttr(e);
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

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

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

  /*
  Left overs...

    @Override public ResultSet executeQuery() throws SQLException {
    return executeQuery(statement.sql);
  }

  @Override public int executeUpdate() throws SQLException {
    return executeUpdate(statement.sql);
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

  @Override public boolean execute() throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    try {
      var response = checkResponse(client.execute(statement));
      var result = response.results.get(0);
      if (result.columns != null && !result.columns.isEmpty()) {
        currentResultSet = new L4Rs(result, this);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new SQLException(format("Execution failed: %s", e.getMessage()), SqlStateConnectionError, e);
    }
  }

  @Override
  public void addBatch() throws SQLException {

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

  }

  @Override public void setRef(int parameterIndex, Ref x) throws SQLException {
    checkClosed();
    notSupported("SQL REF type");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {

  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {

  }

  @Override public void setArray(int parameterIndex, Array x) throws SQLException {
    checkClosed();
    notSupported("SQL ARRAY type");
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

  @Override public void setRowId(int parameterIndex, RowId x) throws SQLException {
    checkClosed();
    notSupported("SQL ROWID type");
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

  @Override public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
    checkClosed();
    notSupported("SQL XML type");
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

  private L4Response checkResponse(L4Response response) throws SQLException {
    if (response.statusCode != 200) {
      var error = response.results != null
        ? response.results.get(0).error
        : format("Unknown error [%d]", response.statusCode);
      throw new SQLException(error, SqlStateGeneralError);
    }
    return response;
  }

  @Override public ResultSet executeQuery(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    try {
      this.statement.sql(sql);
      var response = checkResponse(client.query(statement));
      currentResultSet = new L4Rs(response.results.get(0), this);
      return currentResultSet;
    } catch (Exception e) {
      throw new SQLException(format("Query execution failed: %s", e.getMessage()), SqlStateConnectionError, e);
    }
  }

  @Override public int executeUpdate(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    try {
      var response = checkResponse(client.execute(statement));
      L4Result result = response.results.get(0);
      return result.rowsAffected;
    } catch (Exception e) {
      throw new SQLException(format("Update execution failed: %s", e.getMessage()), SqlStateConnectionError, e);
    }
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

  @Override public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();
    notSupported("Maximum field size");
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

  @Override public void setCursorName(String name) throws SQLException {
    checkClosed();
    notSupported("Positioned updates via cursor name");
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

  @Override public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    notSupported("Fetch direction (scrollable result sets)");
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

  @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    notSupported("executeUpdate(String sql, int autoGeneratedKeys) is not supported for PreparedStatement; use executeUpdate()");
    return -1;
  }

  @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    notSupported("executeUpdate(String sql, int[] columnIndexes) is not supported for PreparedStatement; use executeUpdate()");
    return -1;
  }

  @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    notSupported("executeUpdate(String sql, String[] columnNames) is not supported for PreparedStatement; use executeUpdate()");
    return -1;
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
   */

}
