package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

import static io.vacco.l4zr.jdbc.L4Err.*;
import static io.vacco.l4zr.jdbc.L4Jdbc.*;

public class L4DbMeta implements DatabaseMetaData {

  private final L4Client client;
  private final Connection connection; // Optional, can be null if not provided

  public L4DbMeta(L4Client client, Connection connection) {
    this.client = Objects.requireNonNull(client);
    this.connection = connection; // May be null
  }

  public L4DbMeta(L4Client client) {
    this(client, null);
  }

  private ResultSet executeQuery(String sql) throws SQLException {
    return new L4St(client).executeQuery(sql);
  }

  private boolean matchesPattern(String value, String pattern) {
    if (pattern == null || pattern.equals("%")) {
      return true;
    }
    if (value == null) {
      return false;
    }
    // Convert SQL LIKE pattern to regex (e.g., % -> .*, _ -> .)
    var regex = pattern.replace("%", ".*").replace("_", ".");
    return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(value).matches();
  }

  // General Database Properties

  @Override public boolean allProceduresAreCallable() {
    return false; // Rqlite/SQLite does not support stored procedures
  }

  @Override public boolean allTablesAreSelectable() {
    return true; // All tables can be queried by default in SQLite
  }

  @Override public String getURL() {
    return client.getBaseUrl();
  }

  @Override public String getUserName() {
    return "rqlite"; // Rqlite does not expose user info, use default. TODO shouldn't this be the authenticated http user?
  }

  @Override public boolean isReadOnly() throws SQLException {
    try (var rs = executeQuery("PRAGMA writable_schema")) {
      if (rs.next()) {
        return rs.getInt(1) == 0; // 0 means read-only
      }
      return false; // Default to writable
    }
  }

  @Override public boolean nullsAreSortedHigh() {
    return false; // SQLite sorts NULLs low by default
  }

  @Override public boolean nullsAreSortedLow() {
    return true; // SQLite sorts NULLs low
  }

  @Override public boolean nullsAreSortedAtStart() {
    return true; // SQLite sorts NULLs at the start (low)
  }

  @Override public boolean nullsAreSortedAtEnd() {
    return false; // SQLite does not sort NULLs at the end
  }

  @Override public String getDatabaseProductName() {
    return "Rqlite (SQLite)";
  }

  @Override public String getDatabaseProductVersion() throws SQLException {
    try (var rs = executeQuery("SELECT sqlite_version()")) {
      if (rs.next()) {
        return rs.getString(1);
      }
      return "unknown";
    }
  }

  @Override public String getDriverName() {
    return "l4zr/rqlite";
  }

  @Override public String getDriverVersion() {
    return "1.0"; // TODO Placeholder, update with actual driver version
  }

  @Override public int getDriverMajorVersion() {
    return 1; // TODO parse from jar version file
  }

  @Override public int getDriverMinorVersion() {
    return 0; // TODO parse from jar version file
  }

  @Override public boolean usesLocalFiles() {
    return false; // Rqlite is distributed, not local file-based
  }

  @Override public boolean usesLocalFilePerTable() {
    return false; // Rqlite does not use per-table files
  }

  @Override public boolean supportsMixedCaseIdentifiers() {
    return true; // SQLite is case-sensitive for identifiers
  }

  @Override public boolean storesUpperCaseIdentifiers() {
    return false; // SQLite preserves case
  }

  @Override public boolean storesLowerCaseIdentifiers() {
    return false; // SQLite preserves case
  }

  @Override public boolean storesMixedCaseIdentifiers() {
    return true; // SQLite preserves mixed case
  }

  @Override public boolean supportsMixedCaseQuotedIdentifiers() {
    return true; // SQLite supports quoted identifiers with mixed case
  }

  @Override public boolean storesUpperCaseQuotedIdentifiers() {
    return false; // SQLite preserves case in quotes
  }

  @Override public boolean storesLowerCaseQuotedIdentifiers() {
    return false; // SQLite preserves case in quotes
  }

  @Override public boolean storesMixedCaseQuotedIdentifiers() {
    return true; // SQLite preserves mixed case in quotes
  }

  @Override public String getIdentifierQuoteString() {
    return "\""; // SQLite uses double quotes for identifiers
  }

  @Override public String getSQLKeywords() {
    // TODO SQLite keywords (subset, verify/extend as needed)
    return "ABORT,AFTER,ANALYZE,ATTACH,AUTOINCREMENT,BEFORE,BEGIN,COMMIT,CONFLICT," +
      "DATABASE,DEFERRED,DETACH,EXCLUSIVE,EXPLAIN,FAIL,IMMEDIATE,INDEXED,INSTEAD," +
      "ISNULL,NOTNULL,PLAN,PRAGMA,RECURSIVE,REINDEX,RELEASE,RENAME,REPLACE,RESTRICT," +
      "ROLLBACK,SAVEPOINT,TEMP,TEMPORARY,TRANSACTION,TRIGGER,VACUUM,VIEW,VIRTUAL";
  }

  @Override public String getNumericFunctions() {
    return "ABS,COALESCE,MAX,MIN,RANDOM,ROUND,SUM,TOTAL";
  }

  @Override public String getStringFunctions() {
    return "LENGTH,LOWER,UPPER,TRIM,LTRIM,RTRIM,REPLACE,SUBSTR,INSTR";
  }

  @Override public String getSystemFunctions() {
    return "IFNULL,NULLIF,QUOTE,SQLITE_VERSION";
  }

  @Override public String getTimeDateFunctions() {
    return "DATE,TIME,DATETIME,JULIANDAY,STRFTIME";
  }

  @Override public String getSearchStringEscape() {
    return "\\"; // SQLite uses backslash for LIKE escape
  }

  @Override public String getExtraNameCharacters() {
    return ""; // SQLite allows standard alphanumeric and underscores
  }

  // SQL Feature Support

  @Override public boolean supportsAlterTableWithAddColumn() {
    return true; // SQLite supports ALTER TABLE ADD COLUMN
  }

  @Override public boolean supportsAlterTableWithDropColumn() {
    return false; // SQLite does not support DROP COLUMN
  }

  @Override public boolean supportsColumnAliasing() {
    return true; // SQLite supports AS for column aliases
  }

  @Override public boolean nullPlusNonNullIsNull() {
    return true; // SQLite follows SQL standard for NULL arithmetic
  }

  @Override public boolean supportsConvert() {
    return false; // SQLite does not support CONVERT function
  }

  @Override public boolean supportsConvert(int fromType, int toType) {
    return false; // No CONVERT support
  }

  @Override public boolean supportsTableCorrelationNames() {
    return true; // SQLite supports table aliases
  }

  @Override public boolean supportsDifferentTableCorrelationNames() {
    return true; // SQLite allows different correlation names
  }

  @Override public boolean supportsExpressionsInOrderBy() {
    return true; // SQLite supports expressions in ORDER BY
  }

  @Override public boolean supportsOrderByUnrelated() {
    return true; // SQLite allows ORDER BY on non-selected columns
  }

  @Override public boolean supportsGroupBy() {
    return true; // SQLite supports GROUP BY
  }

  @Override public boolean supportsGroupByUnrelated() {
    return true; // SQLite allows GROUP BY on non-selected columns
  }

  @Override public boolean supportsGroupByBeyondSelect() {
    return true; // SQLite allows GROUP BY with non-selected columns
  }

  @Override public boolean supportsLikeEscapeClause() {
    return true; // SQLite supports ESCAPE in LIKE
  }

  @Override public boolean supportsMultipleResultSets() {
    return false; // SQLite does not support multiple result sets
  }

  @Override public boolean supportsMultipleTransactions() {
    return true; // SQLite supports multiple transactions
  }

  @Override public boolean supportsNonNullableColumns() {
    return true; // SQLite supports NOT NULL constraints
  }

  @Override public boolean supportsMinimumSQLGrammar() {
    return true; // SQLite supports basic SQL
  }

  @Override public boolean supportsCoreSQLGrammar() {
    return true; // SQLite supports core SQL features
  }

  @Override public boolean supportsExtendedSQLGrammar() {
    return false; // SQLite has limited extended SQL support
  }

  @Override public boolean supportsANSI92EntryLevelSQL() {
    return true; // SQLite supports ANSI-92 entry level
  }

  @Override public boolean supportsANSI92IntermediateSQL() {
    return false; // SQLite does not fully support intermediate level
  }

  @Override public boolean supportsANSI92FullSQL() {
    return false; // SQLite does not support full ANSI-92
  }

  @Override public boolean supportsIntegrityEnhancementFacility() {
    return true; // SQLite supports foreign keys and constraints
  }

  @Override public boolean supportsOuterJoins() {
    return true; // SQLite supports LEFT OUTER JOIN
  }

  @Override public boolean supportsFullOuterJoins() {
    return false; // SQLite does not support FULL OUTER JOIN
  }

  @Override public boolean supportsLimitedOuterJoins() {
    return true; // SQLite supports LEFT OUTER JOIN
  }

  @Override public String getSchemaTerm() {
    return "schema"; // Not used in SQLite, but provide term
  }

  @Override public String getProcedureTerm() {
    return "procedure"; // Not supported, but provide term
  }

  @Override public String getCatalogTerm() {
    return "database"; // SQLite uses databases, not catalogs
  }

  @Override public boolean isCatalogAtStart() {
    return true; // SQLite database name precedes table name
  }

  @Override public String getCatalogSeparator() {
    return "."; // SQLite uses dot for database.table
  }

  @Override public boolean supportsSchemasInDataManipulation() {
    return false; // SQLite does not support schemas
  }

  @Override public boolean supportsSchemasInProcedureCalls() {
    return false; // No stored procedures
  }

  @Override public boolean supportsSchemasInTableDefinitions() {
    return false; // No schemas
  }

  @Override public boolean supportsSchemasInIndexDefinitions() {
    return false; // No schemas
  }

  @Override public boolean supportsSchemasInPrivilegeDefinitions() {
    return false; // No schemas
  }

  @Override public boolean supportsCatalogsInDataManipulation() {
    return true; // SQLite supports database name in queries
  }

  @Override public boolean supportsCatalogsInProcedureCalls() {
    return false; // No stored procedures
  }

  @Override public boolean supportsCatalogsInTableDefinitions() {
    return true; // SQLite supports database name in table definitions
  }

  @Override public boolean supportsCatalogsInIndexDefinitions() {
    return true; // SQLite supports database name in index definitions
  }

  @Override public boolean supportsCatalogsInPrivilegeDefinitions() {
    return false; // SQLite does not support privilege definitions
  }

  @Override public boolean supportsPositionedDelete() {
    return false; // SQLite does not support positioned updates/deletes
  }

  @Override public boolean supportsPositionedUpdate() {
    return false; // SQLite does not support positioned updates/deletes
  }

  @Override public boolean supportsSelectForUpdate() {
    return false; // SQLite does not support FOR UPDATE
  }

  @Override public boolean supportsStoredProcedures() {
    return false; // SQLite does not support stored procedures
  }

  @Override public boolean supportsSubqueriesInComparisons() {
    return true; // SQLite supports subqueries in comparisons
  }

  @Override public boolean supportsSubqueriesInExists() {
    return true; // SQLite supports EXISTS subqueries
  }

  @Override public boolean supportsSubqueriesInIns() {
    return true; // SQLite supports IN subqueries
  }

  @Override public boolean supportsSubqueriesInQuantifieds() {
    return true; // SQLite supports quantified subqueries
  }

  @Override public boolean supportsCorrelatedSubqueries() {
    return true; // SQLite supports correlated subqueries
  }

  @Override public boolean supportsUnion() {
    return true; // SQLite supports UNION
  }

  @Override public boolean supportsUnionAll() {
    return true; // SQLite supports UNION ALL
  }

  @Override public boolean supportsOpenCursorsAcrossCommit() {
    return false; // SQLite does not support cursors
  }

  @Override public boolean supportsOpenCursorsAcrossRollback() {
    return false; // SQLite does not support cursors
  }

  @Override public boolean supportsOpenStatementsAcrossCommit() {
    return true; // SQLite allows open statements across commits
  }

  @Override public boolean supportsOpenStatementsAcrossRollback() {
    return true; // SQLite allows open statements across rollbacks
  }

  // Limits and Constraints

  @Override public int getMaxBinaryLiteralLength() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxCharLiteralLength() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnNameLength()  {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnsInGroupBy() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnsInIndex() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnsInOrderBy() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnsInSelect() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxColumnsInTable() {
    return 2000; // SQLite limit
  }

  @Override public int getMaxConnections() {
    return 0; // Rqlite limit depends on configuration, unknown
  }

  @Override public int getMaxCursorNameLength() {
    return 0; // SQLite does not support cursors
  }

  @Override public int getMaxIndexLength() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxSchemaNameLength() {
    return 0; // Schemas not supported
  }

  @Override public int getMaxProcedureNameLength() {
    return 0; // Procedures not supported
  }

  @Override public int getMaxCatalogNameLength() {
    return 0; // No specific limit for database names
  }

  @Override public int getMaxRowSize() {
    return 0; // No specific limit in SQLite
  }

  @Override public boolean doesMaxRowSizeIncludeBlobs() {
    return true; // SQLite includes BLOBs in row size
  }

  @Override public int getMaxStatementLength() {
    return 1000000; // SQLite default limit (1MB)
  }

  @Override public int getMaxStatements() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxTableNameLength() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxTablesInSelect() {
    return 0; // No specific limit in SQLite
  }

  @Override public int getMaxUserNameLength() {
    return 0; // Rqlite does not expose user info
  }

  // Transaction Support

  @Override public int getDefaultTransactionIsolation() {
    return Connection.TRANSACTION_SERIALIZABLE; // SQLite default
  }

  @Override public boolean supportsTransactions() {
    return true; // SQLite supports transactions
  }

  @Override public boolean supportsTransactionIsolationLevel(int level) {
    return level == Connection.TRANSACTION_SERIALIZABLE; // SQLite only supports SERIALIZABLE
  }

  @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return true; // SQLite supports both in transactions
  }

  @Override public boolean supportsDataManipulationTransactionsOnly() {
    return false; // SQLite supports DDL in transactions
  }

  @Override public boolean dataDefinitionCausesTransactionCommit() {
    return false; // SQLite does not auto-commit DDL
  }

  @Override public boolean dataDefinitionIgnoredInTransactions() {
    return false; // SQLite processes DDL in transactions
  }

  // Metadata Queries

  @Override public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    // SQLite does not support stored procedures, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, " +
      "NULL AS PROCEDURE_NAME, NULL AS RESERVED1, NULL AS RESERVED2, NULL AS RESERVED3, " +
      "NULL AS REMARKS, 0 AS PROCEDURE_TYPE, NULL AS SPECIFIC_NAME) WHERE 1=0");
  }

  @Override public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    // No stored procedures, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, " +
      "NULL AS PROCEDURE_NAME, NULL AS COLUMN_NAME, 0 AS COLUMN_TYPE, 0 AS DATA_TYPE, " +
      "NULL AS TYPE_NAME, 0 AS PRECISION, 0 AS LENGTH, 0 AS SCALE, 0 AS RADIX, " +
      "0 AS NULLABLE, NULL AS REMARKS, NULL AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, " +
      "0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, " +
      "NULL AS IS_NULLABLE, NULL AS SPECIFIC_NAME) WHERE 1=0");
  }

  @Override public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    // SQLite does not support schemas or catalogs, treat catalog as database name
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, " +
        "NULL AS TABLE_NAME, NULL AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, " +
        "NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, " +
        "NULL AS REF_GENERATION) WHERE 1=0");
    }

    // Handle table types (TABLE, VIEW, or null for all)
    var typeSet = types == null
      ? new HashSet<>(Arrays.asList("TABLE", "VIEW"))
      : new HashSet<>(Arrays.asList(types));
    var typeFilter = "";
    if (!typeSet.contains("TABLE") && typeSet.contains("VIEW")) {
      typeFilter = " WHERE type = 'view'";
    } else if (typeSet.contains("TABLE") && !typeSet.contains("VIEW")) {
      typeFilter = " WHERE type = 'table'";
    }

    var sql = String.format(
      "SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, name AS TABLE_NAME, " +
        "type AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, " +
        "NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION " +
        "FROM sqlite_master%s " +
        "WHERE name LIKE '%s' AND (type = 'table' OR type = 'view')",
      typeFilter, tableNamePattern == null ? "%" : tableNamePattern.replace("'", "''")
    );

    var rs = executeQuery(sql);
    // Filter by catalog (database name) if specified
    if (catalog != null && !catalog.isEmpty()) {
      var filteredRows = new ArrayList<List<String>>();
      while (rs.next()) {
        if (matchesPattern(catalog, rs.getString("TABLE_CAT"))) {
          filteredRows.add(
            listOf(
              rs.getString("TABLE_CAT"), rs.getString("TABLE_SCHEM"),
              rs.getString("TABLE_NAME"), rs.getString("TABLE_TYPE"),
              rs.getString("REMARKS"), rs.getString("TYPE_CAT"),
              rs.getString("TYPE_SCHEM"), rs.getString("TYPE_NAME"),
              rs.getString("SELF_REFERENCING_COL_NAME"), rs.getString("REF_GENERATION")
            )
          );
        }
      }
      rs.close();
      return new L4Rs(new L4Result(
        listOf("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
          "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
          "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
          ),
        repeat("VARCHAR", 10),
        filteredRows
      ), null);
    }
    return rs;
  }

  @Override public ResultSet getSchemas() throws SQLException {
    // SQLite does not support schemas, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_SCHEM, NULL AS TABLE_CATALOG) WHERE 1=0");
  }

  @Override public ResultSet getCatalogs() throws SQLException {
    // SQLite supports multiple databases, list attached databases
    var rs = executeQuery("PRAGMA database_list");
    var catalogs = new ArrayList<List<String>>();
    while (rs.next()) {
      var name = rs.getString("name");
      if (!name.equals("temp")) { // Exclude temporary database
        catalogs.add(listOf(name));
      }
    }
    rs.close();
    return new L4Rs(
      new L4Result(
        listOf("TABLE_CAT"),
        listOf("VARCHAR"), catalogs
      ), null
    );
  }

  @Override public ResultSet getTableTypes() {
    // SQLite supports TABLE and VIEW
    return new L4Rs(
      new L4Result(
        listOf("TABLE_TYPE"),
        listOf("VARCHAR"),
        listOf(listOf("TABLE"), listOf("VIEW"))
      ), null
    );
  }

  @Override public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    // SQLite does not support schemas
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
        "NULL AS COLUMN_NAME, 0 AS DATA_TYPE, NULL AS TYPE_NAME, 0 AS COLUMN_SIZE, " +
        "0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE, " +
        "NULL AS REMARKS, NULL AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, " +
        "0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, NULL AS IS_NULLABLE, NULL AS SCOPE_CATALOG, " +
        "NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, 0 AS SOURCE_DATA_TYPE, NULL AS IS_AUTOINCREMENT, " +
        "NULL AS IS_GENERATEDCOLUMN) WHERE 1=0");
    }

    // Get all tables matching tableNamePattern
    var tables = getTables(catalog, null, tableNamePattern, new String[]{"TABLE", "VIEW"});
    var columns = new ArrayList<List<String>>();
    while (tables.next()) {
      var tableName = tables.getString("TABLE_NAME");
      var tableCatalog = tables.getString("TABLE_CAT");
      var rs = executeQuery(String.format("PRAGMA table_info('%s')", tableName.replace("'", "''")));
      int ordinal = 1;
      while (rs.next()) {
        var colName = rs.getString("name");
        if (!matchesPattern(colName, columnNamePattern)) {
          continue;
        }
        var type = rs.getString("type").toUpperCase();
        var notNull = rs.getInt("notnull") == 1;
        var defaultValue = rs.getString("dflt_value");
        var isAutoIncrement = rs.getString("pk") != null
          && rs.getString("pk").equals("1")
          && type.contains("INTEGER") && defaultValue == null;

        // Map SQLite type to JDBC type
        int sqlType;
        String typeName = type;
        int columnSize = 0;
        int decimalDigits = 0;
        if (type.contains("INT")) {
          sqlType = Types.INTEGER;
          columnSize = 10;
        } else if (type.contains("REAL") || type.contains("FLOA") || type.contains("DOUB")) {
          sqlType = Types.DOUBLE;
          columnSize = 15;
        } else if (type.contains("TEXT") || type.contains("CHAR") || type.contains("CLOB")) {
          sqlType = Types.VARCHAR;
          columnSize = 0; // No fixed limit
        } else if (type.contains("BLOB")) {
          sqlType = Types.BLOB;
          columnSize = 0; // No fixed limit
        } else if (type.contains("NUMERIC") || type.contains("DECIMAL")) {
          sqlType = Types.NUMERIC;
          columnSize = 15;
          decimalDigits = 5;
        } else if (type.contains("BOOL")) {
          sqlType = Types.BOOLEAN;
          columnSize = 1;
        } else if (type.contains("DATE") || type.contains("DATETIME")) {
          sqlType = Types.DATE;
          columnSize = 10;
        } else if (type.contains("TIMESTAMP")) {
          sqlType = Types.TIMESTAMP;
          columnSize = 19;
        } else {
          sqlType = Types.OTHER;
        }

        columns.add(listOf(
          tableCatalog, // TABLE_CAT
          null, // TABLE_SCHEM
          tableName, // TABLE_NAME
          colName, // COLUMN_NAME
          sqlType, // DATA_TYPE
          typeName, // TYPE_NAME
          Integer.toString(columnSize), // COLUMN_SIZE
          Integer.toString(0), // BUFFER_LENGTH (not used)
          Integer.toString(decimalDigits), // DECIMAL_DIGITS
          Integer.toString(10), // NUM_PREC_RADIX
          notNull ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable, // NULLABLE
          null, // REMARKS
          defaultValue, // COLUMN_DEF
          sqlType, // SQL_DATA_TYPE
          0, // SQL_DATETIME_SUB
          columnSize, // CHAR_OCTET_LENGTH
          ordinal++, // ORDINAL_POSITION
          notNull ? "NO" : "YES", // IS_NULLABLE
          null, // SCOPE_CATALOG
          null, // SCOPE_SCHEMA
          null, // SCOPE_TABLE
          0, // SOURCE_DATA_TYPE
          isAutoIncrement ? "YES" : "NO", // IS_AUTOINCREMENT
          "NO" // IS_GENERATEDCOLUMN
        ));
      }
      rs.close();
    }
    tables.close();
    return new L4Rs(columns, new String[]{
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
      "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
      "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
      "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
      "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
    }, null);
  }

  @Override public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    // SQLite does not support column privileges, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
      "NULL AS COLUMN_NAME, NULL AS GRANTOR, NULL AS GRANTEE, NULL AS PRIVILEGE, NULL AS IS_GRANTABLE) WHERE 1=0");
  }

  @Override public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    // SQLite does not support table privileges, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
      "NULL AS GRANTOR, NULL AS GRANTEE, NULL AS PRIVILEGE, NULL AS IS_GRANTABLE) WHERE 1=0");
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    // Use primary key columns as best row identifier
    ResultSet pkRs = getPrimaryKeys(catalog, schema, table);
    var identifiers = new ArrayList<Object[]>();
    while (pkRs.next()) {
      String colName = pkRs.getString("COLUMN_NAME");
      ResultSet colRs = getColumns(catalog, schema, table, colName);
      if (colRs.next()) {
        int nullableFlag = colRs.getInt("NULLABLE");
        if (!nullable || nullableFlag == DatabaseMetaData.columnNoNulls) {
          identifiers.add(new Object[]{
            colRs.getString("TABLE_CAT"), // SCOPE
            colRs.getString("COLUMN_NAME"), // COLUMN_NAME
            colRs.getInt("DATA_TYPE"), // DATA_TYPE
            colRs.getString("TYPE_NAME"), // TYPE_NAME
            colRs.getInt("COLUMN_SIZE"), // COLUMN_SIZE
            colRs.getInt("BUFFER_LENGTH"), // BUFFER_LENGTH
            colRs.getInt("DECIMAL_DIGITS"), // DECIMAL_DIGITS
            DatabaseMetaData.bestRowSession // PSEUDO_COLUMN
          });
        }
      }
      colRs.close();
    }
    pkRs.close();
    return new L4Rs(identifiers, new String[]{
      "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
      "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"
    }, null);
  }

  @Override public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    // SQLite does not support version columns, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS SCOPE, NULL AS COLUMN_NAME, 0 AS DATA_TYPE, " +
      "NULL AS TYPE_NAME, 0 AS COLUMN_SIZE, 0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, " +
      "0 AS PSEUDO_COLUMN) WHERE 1=0");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    // SQLite does not support schemas
    if (schema != null && !schema.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
        "NULL AS COLUMN_NAME, 0 AS KEY_SEQ, NULL AS PK_NAME) WHERE 1=0");
    }
    if (table == null || table.isEmpty()) {
      throw badQuery("Table name is required");
    }
    ResultSet rs = executeQuery(String.format("PRAGMA table_info('%s')", table.replace("'", "''")));
    var primaryKeys = new ArrayList<Object[]>();
    int keySeq = 1;
    while (rs.next()) {
      if (rs.getInt("pk") > 0) {
        primaryKeys.add(new Object[]{
          catalog, // TABLE_CAT
          null, // TABLE_SCHEM
          table, // TABLE_NAME
          rs.getString("name"), // COLUMN_NAME
          keySeq++, // KEY_SEQ
          "PRIMARY" // PK_NAME
        });
      }
    }
    rs.close();
    return new L4Rs(primaryKeys, new String[]{
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"
    }, null);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    // SQLite does not support schemas
    if (schema != null && !schema.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, " +
        "NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, " +
        "NULL AS FKCOLUMN_NAME, 0 AS KEY_SEQ, 0 AS UPDATE_RULE, 0 AS DELETE_RULE, " +
        "NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY) WHERE 1=0");
    }
    if (table == null || table.isEmpty()) {
      throw badQuery("Table name is required");
    }
    ResultSet rs = executeQuery(String.format("PRAGMA foreign_key_list('%s')", table.replace("'", "''")));
    var foreignKeys = new ArrayList<Object[]>();
    while (rs.next()) {
      foreignKeys.add(new Object[]{
        catalog, // PKTABLE_CAT
        null, // PKTABLE_SCHEM
        rs.getString("table"), // PKTABLE_NAME
        rs.getString("to"), // PKCOLUMN_NAME
        catalog, // FKTABLE_CAT
        null, // FKTABLE_SCHEM
        table, // FKTABLE_NAME
        rs.getString("from"), // FKCOLUMN_NAME
        rs.getInt("seq") + 1, // KEY_SEQ
        DatabaseMetaData.importedKeyNoAction, // UPDATE_RULE (SQLite does not support ON UPDATE)
        rs.getString("on_delete") != null && rs.getString("on_delete").equals("CASCADE")
          ? DatabaseMetaData.importedKeyCascade : DatabaseMetaData.importedKeyNoAction, // DELETE_RULE
        "FK_" + table + "_" + rs.getString("from"), // FK_NAME (synthetic)
        "PRIMARY", // PK_NAME
        DatabaseMetaData.importedKeyNotDeferrable // DEFERRABILITY
      });
    }
    rs.close();
    return new L4Rs(foreignKeys, new String[]{
      "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
      "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
      "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
    }, null);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    // SQLite does not support schemas
    if (schema != null && !schema.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, " +
        "NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, " +
        "NULL AS FKCOLUMN_NAME, 0 AS KEY_SEQ, 0 AS UPDATE_RULE, 0 AS DELETE_RULE, " +
        "NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY) WHERE 1=0");
    }
    if (table == null || table.isEmpty()) {
      throw badQuery("Table name is required");
    }
    // Find all tables with foreign keys referencing this table
    ResultSet tables = getTables(catalog, null, "%", new String[]{"TABLE"});
    var exportedKeys = new ArrayList<Object[]>();
    while (tables.next()) {
      String fkTable = tables.getString("TABLE_NAME");
      ResultSet fkRs = executeQuery(String.format("PRAGMA foreign_key_list('%s')", fkTable.replace("'", "''")));
      while (fkRs.next()) {
        if (table.equals(fkRs.getString("table"))) {
          exportedKeys.add(new Object[]{
            catalog, // PKTABLE_CAT
            null, // PKTABLE_SCHEM
            table, // PKTABLE_NAME
            fkRs.getString("to"), // PKCOLUMN_NAME
            catalog, // FKTABLE_CAT
            null, // FKTABLE_SCHEM
            fkTable, // FKTABLE_NAME
            fkRs.getString("from"), // FKCOLUMN_NAME
            fkRs.getInt("seq") + 1, // KEY_SEQ
            DatabaseMetaData.importedKeyNoAction, // UPDATE_RULE
            fkRs.getString("on_delete") != null && fkRs.getString("on_delete").equals("CASCADE")
              ? DatabaseMetaData.importedKeyCascade : DatabaseMetaData.importedKeyNoAction, // DELETE_RULE
            "FK_" + fkTable + "_" + fkRs.getString("from"), // FK_NAME (synthetic)
            "PRIMARY", // PK_NAME
            DatabaseMetaData.importedKeyNotDeferrable // DEFERRABILITY
          });
        }
      }
      fkRs.close();
    }
    tables.close();
    return new L4Rs(exportedKeys, new String[]{
      "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
      "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
      "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
    }, null);
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                     String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    // SQLite does not support schemas
    if (parentSchema != null && !parentSchema.isEmpty() || foreignSchema != null && !foreignSchema.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, " +
        "NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, NULL AS FKTABLE_NAME, " +
        "NULL AS FKCOLUMN_NAME, 0 AS KEY_SEQ, 0 AS UPDATE_RULE, 0 AS DELETE_RULE, " +
        "NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY) WHERE 1=0");
    }
    ResultSet fkRs = getImportedKeys(foreignCatalog, foreignSchema, foreignTable);
    var crossRefs = new ArrayList<Object[]>();
    while (fkRs.next()) {
      if (parentTable.equals(fkRs.getString("PKTABLE_NAME")) &&
        (parentCatalog == null || parentCatalog.equals(fkRs.getString("PKTABLE_CAT")))) {
        crossRefs.add(new Object[]{
          fkRs.getString("PKTABLE_CAT"),
          fkRs.getString("PKTABLE_SCHEM"),
          fkRs.getString("PKTABLE_NAME"),
          fkRs.getString("PKCOLUMN_NAME"),
          fkRs.getString("FKTABLE_CAT"),
          fkRs.getString("FKTABLE_SCHEM"),
          fkRs.getString("FKTABLE_NAME"),
          fkRs.getString("FKCOLUMN_NAME"),
          fkRs.getInt("KEY_SEQ"),
          fkRs.getInt("UPDATE_RULE"),
          fkRs.getInt("DELETE_RULE"),
          fkRs.getString("FK_NAME"),
          fkRs.getString("PK_NAME"),
          fkRs.getInt("DEFERRABILITY")
        });
      }
    }
    fkRs.close();
    return new L4Rs(crossRefs, new String[]{
      "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
      "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
      "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
    }, null);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    // Common SQLite types
    var types = new ArrayList<Object[]>();
    types.add(new Object[]{"INTEGER", Types.INTEGER, 10, null, null, null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "INTEGER", 0, 0, 10, "YES", null});
    types.add(new Object[]{"TEXT", Types.VARCHAR, 0, "'", "'", null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "TEXT", 0, 0, 0, "YES", null});
    types.add(new Object[]{"REAL", Types.DOUBLE, 15, null, null, null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "REAL", 0, 0, 15, "YES", null});
    types.add(new Object[]{"BLOB", Types.BLOB, 0, null, null, null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "BLOB", 0, 0, 0, "YES", null});
    types.add(new Object[]{"NUMERIC", Types.NUMERIC, 15, null, null, null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "NUMERIC", 0, 5, 10, "YES", null});
    types.add(new Object[]{"BOOLEAN", Types.BOOLEAN, 1, null, null, null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "BOOLEAN", 0, 0, 1, "YES", null});
    types.add(new Object[]{"DATE", Types.DATE, 10, "'", "'", null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "DATE", 0, 0, 10, "YES", null});
    types.add(new Object[]{"TIMESTAMP", Types.TIMESTAMP, 19, "'", "'", null,
      DatabaseMetaData.typeNullable, false, 3, true, false, false,
      "TIMESTAMP", 0, 0, 19, "YES", null});
    return new L4Rs(types, new String[]{
      "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
      "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
      "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
      "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
    }, null);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    // SQLite does not support schemas
    if (schema != null && !schema.isEmpty()) {
      return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
        "0 AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, NULL AS INDEX_NAME, 0 AS TYPE, " +
        "0 AS ORDINAL_POSITION, NULL AS COLUMN_NAME, NULL AS ASC_OR_DESC, 0 AS CARDINALITY, " +
        "0 AS PAGES, NULL AS FILTER_CONDITION) WHERE 1=0");
    }
    if (table == null || table.isEmpty()) {
      throw badQuery("Table name is required");
    }
    ResultSet rs = executeQuery(String.format("PRAGMA index_list('%s')", table.replace("'", "''")));
    var indexes = new ArrayList<Object[]>();
    while (rs.next()) {
      String indexName = rs.getString("name");
      boolean isUnique = rs.getInt("unique") == 1;
      if (unique && !isUnique) {
        continue; // Skip non-unique indexes if unique=true
      }
      ResultSet idxInfo = executeQuery(String.format("PRAGMA index_info('%s')", indexName.replace("'", "''")));
      while (idxInfo.next()) {
        indexes.add(new Object[]{
          catalog, // TABLE_CAT
          null, // TABLE_SCHEM
          table, // TABLE_NAME
          isUnique ? 0 : 1, // NON_UNIQUE
          null, // INDEX_QUALIFIER
          indexName, // INDEX_NAME
          DatabaseMetaData.tableIndexOther, // TYPE
          idxInfo.getInt("seqno") + 1, // ORDINAL_POSITION
          idxInfo.getString("name"), // COLUMN_NAME
          null, // ASC_OR_DESC (SQLite does not provide)
          0, // CARDINALITY (not available)
          0, // PAGES (not available)
          null // FILTER_CONDITION
        });
      }
      idxInfo.close();
    }
    rs.close();
    return new L4Rs(indexes, new String[]{
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
      "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
      "CARDINALITY", "PAGES", "FILTER_CONDITION"
    }, null);
  }

  // ResultSet Support

  @Override public boolean supportsResultSetType(int type) {
    return type == ResultSet.TYPE_FORWARD_ONLY; // SQLite only supports forward-only
  }

  @Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override public boolean ownUpdatesAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean ownDeletesAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean ownInsertsAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean othersUpdatesAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean othersDeletesAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean othersInsertsAreVisible(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean updatesAreDetected(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean deletesAreDetected(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean insertsAreDetected(int type) {
    return false; // SQLite does not support updatable result sets
  }

  @Override public boolean supportsBatchUpdates() {
    return true; // Rqlite supports batch updates
  }

  @Override public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    // SQLite does not support UDTs, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, " +
      "NULL AS CLASS_NAME, 0 AS DATA_TYPE, NULL AS REMARKS, 0 AS BASE_TYPE) WHERE 1=0");
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (connection == null) {
      throw badQuery("No connection available");
    }
    return connection;
  }

  // Advanced Features

  @Override public boolean supportsSavepoints() {
    return true; // SQLite supports SAVEPOINT
  }

  @Override public boolean supportsNamedParameters() {
    return true; // Rqlite supports ? and :name parameters
  }

  @Override public boolean supportsMultipleOpenResults() {
    return false; // SQLite does not support multiple open result sets
  }

  @Override public boolean supportsGetGeneratedKeys() {
    return false;
  }

  @Override public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    // SQLite does not support UDTs, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, " +
      "NULL AS SUPERTYPE_CAT, NULL AS SUPERTYPE_SCHEM, NULL AS SUPERTYPE_NAME) WHERE 1=0");
  }

  @Override public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    // SQLite does not support table inheritance, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
      "NULL AS SUPERTABLE_NAME) WHERE 1=0");
  }

  @Override public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    // SQLite does not support UDTs, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, " +
      "NULL AS ATTR_NAME, 0 AS DATA_TYPE, NULL AS ATTR_TYPE_NAME, 0 AS ATTR_SIZE, " +
      "0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE, NULL AS REMARKS, " +
      "NULL AS ATTR_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH, " +
      "0 AS ORDINAL_POSITION, NULL AS IS_NULLABLE, NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, " +
      "NULL AS SCOPE_TABLE, 0 AS SOURCE_DATA_TYPE) WHERE 1=0");
  }

  @Override public boolean supportsResultSetHoldability(int holdability) {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT; // SQLite closes cursors at commit
  }

  @Override public int getResultSetHoldability() {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override public int getDatabaseMajorVersion() throws SQLException {
    var version = getDatabaseProductVersion();
    return Integer.parseInt(version.split("\\.")[0]);
  }

  @Override public int getDatabaseMinorVersion() throws SQLException {
    var version = getDatabaseProductVersion();
    var parts = version.split("\\.");
    return parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
  }

  @Override public int getJDBCMajorVersion() {
    return 4; // JDBC 4.0
  }

  @Override public int getJDBCMinorVersion() {
    return 0; // JDBC 4.0
  }

  @Override public int getSQLStateType() {
    return DatabaseMetaData.sqlStateSQL; // Use standard SQL state codes
  }

  @Override public boolean locatorsUpdateCopy() {
    return false; // SQLite does not support locators
  }

  @Override public boolean supportsStatementPooling() {
    return false; // SQLite does not support statement pooling
  }

  @Override public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED; // SQLite does not support ROWID as per JDBC
  }

  @Override public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return getSchemas(); // Delegate to getSchemas()
  }

  @Override public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false; // SQLite does not support stored functions
  }

  @Override public boolean autoCommitFailureClosesAllResultSets() {
    return false; // SQLite does not close result sets on auto-commit failure
  }

  @Override public ResultSet getClientInfoProperties() throws SQLException {
    // No client info properties supported
    return executeQuery("SELECT * FROM (SELECT NULL AS NAME, 0 AS MAX_LEN, NULL AS DEFAULT_VALUE, " +
      "NULL AS DESCRIPTION) WHERE 1=0");
  }

  @Override public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    // SQLite does not support stored functions
    return executeQuery("SELECT * FROM (SELECT NULL AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, " +
      "NULL AS FUNCTION_NAME, NULL AS REMARKS, 0 AS FUNCTION_TYPE, NULL AS SPECIFIC_NAME) WHERE 1=0");
  }

  @Override public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    // No stored functions, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, " +
      "NULL AS FUNCTION_NAME, NULL AS COLUMN_NAME, 0 AS COLUMN_TYPE, 0 AS DATA_TYPE, " +
      "NULL AS TYPE_NAME, 0 AS PRECISION, 0 AS LENGTH, 0 AS SCALE, 0 AS RADIX, " +
      "0 AS NULLABLE, NULL AS REMARKS, 0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, " +
      "NULL AS IS_NULLABLE, NULL AS SPECIFIC_NAME) WHERE 1=0");
  }

  @Override public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    // SQLite does not support pseudo columns, return empty result set
    return executeQuery("SELECT * FROM (SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
      "NULL AS COLUMN_NAME, 0 AS DATA_TYPE, 0 AS COLUMN_SIZE, 0 AS DECIMAL_DIGITS, " +
      "0 AS NUM_PREC_RADIX, NULL AS COLUMN_USAGE, NULL AS REMARKS, 0 AS CHAR_OCTET_LENGTH, " +
      "NULL AS IS_NULLABLE) WHERE 1=0");
  }

  @Override public boolean generatedKeyAlwaysReturned() {
    return false;
  }

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == null) {
      throw badInterface();
    }
    if (iface == DatabaseMetaData.class || iface == Wrapper.class) {
      return iface.cast(this);
    }
    throw badUnwrap(iface);
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw badInterface();
    }
    return iface == DatabaseMetaData.class || iface == Wrapper.class;
  }

}
