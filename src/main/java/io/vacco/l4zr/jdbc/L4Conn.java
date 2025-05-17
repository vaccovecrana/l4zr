package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

import static java.lang.String.*;
import static io.vacco.l4zr.jdbc.L4Err.*;

public class L4Conn implements Connection {

  private final L4Client client;
  private final L4DbMeta meta;
  private final Properties clientInfo;

  private boolean isClosed;
  private int holdability;

  private String schema;
  private Map<String, Class<?>> typeMap;

  public L4Conn(L4Client client) throws SQLException {
    if (client == null) {
      throw new SQLException("L4Client cannot be null", SqlStateInvalidParam);
    }
    this.client = client;
    this.isClosed = false;
    this.clientInfo = new Properties();
    this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    this.typeMap = null;
    this.schema = null;
    this.meta = new L4DbMeta(client);
  }

  private void checkClosed() throws SQLException {
    if (isClosed) {
      throw badState("Connection is closed");
    }
  }

  private void validateResultSetParams(int resultSetType, int resultSetConcurrency) throws SQLException {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw notSupported("Only TYPE_FORWARD_ONLY supported");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw notSupported("Only CONCUR_READ_ONLY supported");
    }
  }

  private void validateResultSetParams(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    validateResultSetParams(resultSetType, resultSetConcurrency);
    if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT &&
      resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw badParam("Invalid holdability");
    }
  }

  @Override public Statement createStatement() throws SQLException {
    checkClosed();
    return new L4St(client);
  }

  @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkClosed();
    return new L4Ps(client, sql);
  }

  @Override public CallableStatement prepareCall(String sql) throws SQLException {
    checkClosed();
    throw notSupported("Callable statements not supported");
  }

  @Override public String nativeSQL(String sql) throws SQLException {
    checkClosed();
    return sql;
  }

  @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();
    throw notSupported(join("", "",
      "Auto-commit cannot be changed; rqlite executes all statements atomically.",
      "Use Statement.addBatch() and executeBatch() for transactions."
    ));
  }

  @Override public boolean getAutoCommit() throws SQLException {
    checkClosed();
    return L4Options.transaction;
  }

  @Override public void commit() throws SQLException {
    checkClosed();
    throw notSupported(join("", "",
      "Manual commit not supported; rqlite executes all statements atomically. ",
      "Use Statement.addBatch() and executeBatch() for transactions."
    ));
  }

  @Override public void rollback() throws SQLException {
    checkClosed();
    throw notSupported("Rollback not supported");
  }

  @Override public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    isClosed = true;
    this.client.close();
  }

  @Override public boolean isClosed() {
    return isClosed;
  }

  @Override public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    return meta;
  }

  @Override public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed(); // no-op
  }

  @Override public boolean isReadOnly() throws SQLException {
    checkClosed();
    return false;
  }

  @Override public void setCatalog(String catalog) throws SQLException {
    checkClosed();
    throw notSupported("Catalog switching");
  }

  @Override public String getCatalog() throws SQLException {
    checkClosed();
    return "";
  }

  @Override public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();
    if (level != TRANSACTION_SERIALIZABLE) {
      throw notSupported(format("transaction isolation level: [%d]", level));
    }
  }

  @Override public int getTransactionIsolation() throws SQLException {
    checkClosed();
    return TRANSACTION_SERIALIZABLE;
  }

  @Override public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    return null;
  }

  @Override public void clearWarnings() throws SQLException {
    checkClosed();
  }

  @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency);
    return new L4St(client);
  }

  @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
    throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency);
    return new L4Ps(client, sql);
  }

  @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    checkClosed();
    throw notSupported("Callable statements");
  }

  @Override public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();
    return typeMap != null ? typeMap : new java.util.HashMap<>();
  }

  @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    this.typeMap = map;
  }

  @Override public void setHoldability(int holdability) throws SQLException {
    checkClosed();
    if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT &&
      holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw badParam(format("Invalid holdability: [%d]", holdability));
    }
    this.holdability = holdability;
  }

  @Override public int getHoldability() throws SQLException {
    checkClosed();
    return holdability;
  }

  @Override public Savepoint setSavepoint() throws SQLException {
    checkClosed();
    throw notSupported("Savepoints");
  }

  @Override public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    throw notSupported("Savepoints");
  }

  @Override public void rollback(Savepoint savepoint) throws SQLException {
    checkClosed();
    throw notSupported("Savepoints");
  }

  @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    checkClosed();
    throw notSupported("Savepoints");
  }

  @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
    throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new L4St(client);
  }

  @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                      int resultSetHoldability) throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new L4Ps(client, sql);
  }

  @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    checkClosed();
    throw notSupported("Callable statements");
  }

  @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    return new L4Ps(client, sql);
  }

  @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw notSupported("Column index-based key retrieval");
  }

  @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw notSupported("Column name-based key retrieval");
  }

  @Override public Clob createClob() throws SQLException {
    checkClosed();
    return new L4Clob();
  }

  @Override public Blob createBlob() throws SQLException {
    checkClosed();
    return new L4Blob();
  }

  @Override public NClob createNClob() throws SQLException {
    checkClosed();
    return new L4NClob();
  }

  @Override public SQLXML createSQLXML() throws SQLException {
    checkClosed();
    throw notSupported("SQLXML");
  }

  @Override public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw badParam("Timeout cannot be negative");
    }
    if (isClosed) {
      return false;
    }
    try {
      client.status();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      checkClosed();
      clientInfo.setProperty(name, value);
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), e.getSQLState(), null, e);
    }
  }

  @Override public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      checkClosed();
      clientInfo.clear();
      if (properties != null) {
        clientInfo.putAll(properties);
      }
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), e.getSQLState(), null, e);
    }
  }

  @Override public String getClientInfo(String name) throws SQLException {
    checkClosed();
    return clientInfo.getProperty(name);
  }

  @Override public Properties getClientInfo() throws SQLException {
    checkClosed();
    return clientInfo;
  }

  @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkClosed();
    throw notSupported("Arrays");
  }

  @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();
    throw notSupported("Structs");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    checkClosed();
    // rqlite may not support schemas; store or validate if applicable
    this.schema = schema;
  }

  @Override
  public String getSchema() throws SQLException {
    checkClosed();
    return schema;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (isClosed) {
      return;
    }
    if (executor == null) {
      throw new SQLException("Executor cannot be null", SqlStateInvalidParam);
    }
    // Execute cleanup in the provided executor
    executor.execute(() -> {
      try {
        close();
      } catch (SQLException e) {
        // Log error if needed
      }
    });
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    checkClosed();
    if (milliseconds < 0) {
      throw new SQLException("Timeout cannot be negative", SqlStateInvalidParam);
    }
    // Configure network timeout on L4Client if supported
    throw new SQLException("Network timeout not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    checkClosed();
    // Return default or configured timeout
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    checkClosed();
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkClosed();
    return iface.isAssignableFrom(getClass());
  }

}
