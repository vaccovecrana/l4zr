package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.sql.*;
import java.util.*;

import static io.vacco.l4zr.jdbc.L4Err.*;

public class L4St implements Statement {

  protected final L4Client client;
  protected boolean isClosed = false;
  protected L4Rs currentResultSet = null;
  protected int maxRows = -1;
  protected int fetchSize = 0;
  protected final List<L4Statement> batch = new ArrayList<>();

  public L4St(L4Client client) {
    this.client = Objects.requireNonNull(client);
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

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw stClosed();
    }
  }

  protected void closeCurrentResultSet() {
    if (currentResultSet != null && !currentResultSet.isClosed()) {
      currentResultSet.close();
    }
    currentResultSet = null;
  }

  @Override public ResultSet executeQuery(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      var response = client.query(new L4Statement().sql(sql));
      var result = response.results.get(0);
      if (result.isError()) {
        throw new IllegalStateException(result.error);
      }
      currentResultSet = new L4Rs(result, this).clampTo(maxRows);
      return currentResultSet;
    } catch (Exception e) {
      throw badQuery(e);
    }
  }

  @Override public int executeUpdate(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      var response = client.execute(new L4Statement().sql(sql));
      var result = response.results.get(0);
      if (result.isError()) {
        throw new IllegalStateException(result.error);
      }
      return result.rowsAffected;
    } catch (Exception e) {
      throw badUpdate(e);
    }
  }

  @Override public void close() throws SQLException {
    if (!isClosed) {
      closeCurrentResultSet();
      batch.clear();
      isClosed = true;
    }
  }

  @Override public int getMaxFieldSize() throws SQLException {
    checkClosed();
    throw notSupported("Maximum field size");
  }

  @Override public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();
    throw notSupported("Maximum field size");
  }

  @Override public int getMaxRows() throws SQLException {
    checkClosed();
    return maxRows;
  }

  @Override public void setMaxRows(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw badMaxRows();
    }
    this.maxRows = max;
  }

  @Override public void setEscapeProcessing(boolean enable) throws SQLException {
    checkClosed();
    // TODO implement test case for SQL injection
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {

  }

  @Override public void cancel() throws SQLException {
    checkClosed();
    throw notSupported("Statement cancellation");
  }

  @Override public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override public void setCursorName(String name) throws SQLException {
    checkClosed();
    throw notSupported("Positioned updates via cursor name");
  }

  @Override public boolean execute(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      var response = client.execute(new L4Statement().sql(sql));
      var result = response.results.get(0);
      if (result.isError()) {
        throw new IllegalStateException(result.error);
      }
      if (result.columns != null && !result.columns.isEmpty()) {
        currentResultSet = new L4Rs(result, this).clampTo(maxRows);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw badExec(e);
    }
  }

  @Override public ResultSet getResultSet() throws SQLException {
    checkClosed();
    return currentResultSet;
  }

  @Override public int getUpdateCount() throws SQLException {
    checkClosed();
    return currentResultSet == null ? -1 : currentResultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    throw notSupported("Fetch direction (scrollable result sets)");
  }

  @Override public int getFetchDirection() throws SQLException {
    checkClosed();
    throw notSupported("Fetch direction (scrollable result sets)");
  }

  @Override public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    if (rows < 0) {
      throw badFetchSize(rows);
    }
    this.fetchSize = rows;
  }

  @Override public int getFetchSize() throws SQLException {
    checkClosed();
    return fetchSize;
  }

  @Override public int getResultSetConcurrency() throws SQLException {
    checkClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override public int getResultSetType() throws SQLException {
    checkClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override public void addBatch(String sql) throws SQLException {
    checkClosed();
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    batch.add(new L4Statement().sql(sql));
  }

  @Override public void clearBatch() throws SQLException {
    checkClosed();
    batch.clear();
  }

  @Override public int[] executeBatch() throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    if (batch.isEmpty()) {
      return new int[0];
    }
    try {
      var response = client.execute(batch.toArray(new L4Statement[0]));
      var updateCounts = new int[response.results.size()];
      for (int i = 0; i < response.results.size(); i++) {
        var result = response.results.get(i);
        if (result.isError()) {
          throw new BatchUpdateException(result.error, SqlStateGeneralError, updateCounts, null);
        }
        updateCounts[i] = result.rowsAffected;
      }
      batch.clear();
      return updateCounts;
    } catch (Exception e) {
      throw badBatch(e);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();
    throw notSupported("Generated keys");
  }

  @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
      throw notSupported("Generated keys");
    }
    return executeUpdate(sql);
  }

  @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw notSupported("Generated keys by column indexes");
  }

  @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw notSupported("Generated keys by column names");
  }

  @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
      throw notSupported("Generated keys");
    }
    return execute(sql);
  }

  @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw notSupported("Generated keys by column indexes");
  }

  @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw notSupported("Generated keys by column names");
  }

  @Override public int getResultSetHoldability() throws SQLException {
    checkClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override public boolean isClosed() {
    return isClosed;
  }

  @Override public void setPoolable(boolean poolable) throws SQLException {
    checkClosed();
    throw notSupported("Statement pooling");
  }

  @Override public boolean isPoolable() throws SQLException {
    checkClosed();
    throw notSupported("Statement pooling");
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
      throw badInterface();
    }
    if (iface == Statement.class || iface == Wrapper.class) {
      return iface.cast(this);
    }
    throw badUnwrap(iface);
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw badInterface();
    }
    return iface == Statement.class || iface == Wrapper.class;
  }

}
