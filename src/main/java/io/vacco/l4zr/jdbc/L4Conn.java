package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.L4Client;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static io.vacco.l4zr.jdbc.L4Err.*;

public class L4Conn implements Connection {

  private final L4Client client; // rqlite client for database interactions
  private boolean isClosed; // Tracks connection state
  private boolean autoCommit; // Auto-commit mode
  private int transactionIsolation; // Transaction isolation level
  private Properties clientInfo; // Client info properties
  private String catalog; // Current catalog (database)
  private String schema; // Current schema
  private int holdability; // ResultSet holdability
  private SQLWarning warnings; // SQL warnings chain
  private Map<String, Class<?>> typeMap; // Custom type mapping

  /**
   * Constructor initializing the connection with an L4Client.
   *
   * @param client The L4Client instance connected to rqlite
   * @throws SQLException If initialization fails
   */
  public L4Conn(L4Client client) throws SQLException {
    if (client == null) {
      throw new SQLException("L4Client cannot be null", SqlStateInvalidParam);
    }
    this.client = client;
    this.isClosed = false;
    this.autoCommit = true; // Default per JDBC spec
    this.transactionIsolation = Connection.TRANSACTION_READ_COMMITTED; // Default
    this.clientInfo = new Properties();
    this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT; // Default
    this.warnings = null;
    this.typeMap = null;
    this.catalog = null;
    this.schema = null;
  }

  private void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("Connection is closed", SqlStateGeneralError);
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
      throw new SQLException("Invalid holdability", SqlStateInvalidParam);
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

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();
    // rqlite does not support transactions in the traditional sense
    // You may need to implement logic to simulate auto-commit behavior
    this.autoCommit = autoCommit;
    // If autoCommit is false, you might need to start a transaction via client
    if (!autoCommit) {
      // Placeholder: Implement rqlite transaction begin if supported
      throw new SQLException("Transactions not fully supported", SqlStateFeatureNotSupported);
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkClosed();
    return autoCommit;
  }

  @Override public void commit() throws SQLException {
    checkClosed();
    if (autoCommit) {
      throw badState("Cannot commit in auto-commit mode");
    }
    throw new SQLException("Commit not supported", SqlStateFeatureNotSupported);
  }

  @Override public void rollback() throws SQLException {
    checkClosed();
    throw notSupported("Rollback not supported");
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    isClosed = true;
    // Perform any cleanup, e.g., close L4Client resources
    // Note: L4Client may need a close method
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    return new L4DbMeta(client, this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();
    // rqlite may not support read-only mode; log or ignore
    // If supported, configure client accordingly
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();
    // Assume read-write unless client indicates otherwise
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkClosed();
    // rqlite may not support catalogs; store or validate if applicable
    this.catalog = catalog;
  }

  @Override
  public String getCatalog() throws SQLException {
    checkClosed();
    return catalog;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();
    // Validate supported levels; rqlite may only support a subset
    switch (level) {
      case TRANSACTION_NONE:
      case TRANSACTION_READ_COMMITTED:
        this.transactionIsolation = level;
        break;
      default:
        throw new SQLException("Unsupported transaction isolation level", SqlStateFeatureNotSupported);
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkClosed();
    return transactionIsolation;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkClosed();
    warnings = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency);
    return new L4Stmt(client, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
    throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency);
    return new L4Ps(client, sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
    throws SQLException {
    checkClosed();
    throw new SQLException("Callable statements not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();
    return typeMap != null ? typeMap : new java.util.HashMap<>();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    this.typeMap = map;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkClosed();
    if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT &&
      holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLException("Invalid holdability", SqlStateInvalidParam);
    }
    this.holdability = holdability;
  }

  @Override
  public int getHoldability() throws SQLException {
    checkClosed();
    return holdability;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkClosed();
    throw new SQLException("Savepoints not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    throw new SQLException("Savepoints not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    checkClosed();
    throw new SQLException("Savepoints not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    checkClosed();
    throw new SQLException("Savepoints not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
    throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new L4Stmt(client, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    checkClosed();
    validateResultSetParams(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new L4Ps(client, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    checkClosed();
    throw new SQLException("Callable statements not supported", SqlStateFeatureNotSupported);
  }

  @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    return new L4Ps(client, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw new SQLException("Column index-based key retrieval not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw new SQLException("Column name-based key retrieval not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public Clob createClob() throws SQLException {
    checkClosed();
    return new L4Clob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkClosed();
    return new L4Blob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkClosed();
    return new L4NClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    checkClosed();
    throw new SQLException("SQLXML not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("Timeout cannot be negative", SqlStateInvalidParam);
    }
    if (isClosed) {
      return false;
    }
    // Check connection validity, e.g., by querying client status
    try {
      client.status();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      checkClosed();
      clientInfo.setProperty(name, value);
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), e.getSQLState(), e);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      checkClosed();
      clientInfo.clear();
      if (properties != null) {
        clientInfo.putAll(properties);
      }
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), e.getSQLState(), e);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkClosed();
    return clientInfo.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkClosed();
    return clientInfo;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkClosed();
    throw new SQLException("Arrays not supported", SqlStateFeatureNotSupported);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();
    throw new SQLException("Structs not supported", SqlStateFeatureNotSupported);
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