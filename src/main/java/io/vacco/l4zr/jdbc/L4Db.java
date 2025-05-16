package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;

import static io.vacco.l4zr.jdbc.L4Jdbc.*;
import static java.lang.String.*;
import static java.util.stream.Collectors.toList;

public class L4Db {

  public static final String Keywords = join(",",
    "ACTION,ADD,AFTER,ALL,ALTER,ANALYZE,AND,AS,ASC,ATTACH,AUTOINCREMENT,",
    "BEFORE,BEGIN,BETWEEN,BY,",
    "CASCADE,CASE,CAST,CHECK,COLLATE,COLUMN,COMMIT,CONFLICT,CONSTRAINT,CREATE,",
    "CROSS,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,",
    "DATABASE,DEFAULT,DEFERRABLE,DEFERRED,DELETE,DESC,DETACH,DISTINCT,DROP,",
    "EACH,ELSE,END,ESCAPE,EXCEPT,EXCLUSIVE,EXISTS,EXPLAIN,",
    "FAIL,FOR,FOREIGN,FROM,FULL,GLOB,GROUP,HAVING,",
    "IF,IGNORE,IMMEDIATE,IN,INDEX,INDEXED,INITIALLY,INNER,INSERT,INSTEAD,INTERSECT,INTO,IS,ISNULL,",
    "JOIN,KEY,LEFT,LIKE,LIMIT,MATCH,NATURAL,NO,NOT,NOTNULL,NULL,OF,OFFSET,ON,OR,ORDER,OUTER,",
    "PLAN,PRAGMA,PRIMARY,QUERY,RAISE,RECURSIVE,REFERENCES,REGEXP,REINDEX,RELEASE,RENAME,REPLACE,",
    "RESTRICT,RIGHT,ROLLBACK,ROW,SAVEPOINT,SELECT,SET,TABLE,TEMP,TEMPORARY,THEN,TO,TRANSACTION,",
    "TRIGGER,UNION,UNIQUE,UPDATE,USING,VACUUM,VALUES,VIEW,VIRTUAL,WHEN,WHERE,WITH,WITHOUT"
  );

  public static final String FnNumeric  = "abs,coalesce,likelihood,likely,max,min,random,randomblob,round,sign,unlikely,zeroblob";
  public static final String FnString   = "char,concat,concat_ws,format,glob,hex,instr,length,like,lower,ltrim,octet_length,printf,replace,rtrim,soundex,substr,substring,trim,unicode,unhex,upper";
  public static final String FnSystem   = "changes,iif,ifnull,last_insert_rowid,nullif,quote,sqlite_compileoption_get,sqlite_compileoption_used,sqlite_offset,sqlite_source_id,sqlite_version,total_changes,typeof";
  public static final String FnDateTime = "date,datetime,julianday,strftime,time";

  public static final String
    BUFFER_LENGTH = "BUFFER_LENGTH",
    CASCADE = "CASCADE", COLUMN_NAME = "COLUMN_NAME", COLUMN_SIZE = "COLUMN_SIZE",
    DATA_TYPE = "DATA_TYPE", DECIMAL_DIGITS = "DECIMAL_DIGITS", DELETE_RULE = "DELETE_RULE",
    DEFERRABILITY = "DEFERRABILITY",
    FK_NAME = "FK_NAME",
    KEY_SEQ = "KEY_SEQ", NULLABLE = "NULLABLE", UPDATE_RULE = "UPDATE_RULE",

    TABLE = "TABLE", VIEW = "VIEW",
    TABLE_CAT = "TABLE_CAT", TABLE_NAME = "TABLE_NAME", TYPE_NAME = "TYPE_NAME",

    PK_NAME = "PK_NAME",
    PKTABLE_NAME = "PKTABLE_NAME", PKTABLE_CAT = "PKTABLE_CAT", PKTABLE_SCHEM = "PKTABLE_SCHEM",
    PKCOLUMN_NAME = "PKCOLUMN_NAME",

    FKTABLE_CAT = "FKTABLE_CAT", FKTABLE_SCHEM = "FKTABLE_SCHEM", FKTABLE_NAME = "FKTABLE_NAME",
    FKCOLUMN_NAME = "FKCOLUMN_NAME",

    kDesc = "desc", kFrom = "from",
    kName = "name", kOnDelete = "on_delete",
    kTable = "table", kTemp = "temp", kTo = "to",
    kType = "type", kNotNull = "notnull",
    kDfltValue = "dflt_value", kPk = "pk", kSeq = "seq", kSeqNo = "seqno",
    kNull = "null", kUnique = "unique",

    i0 = "0", i1 = "1", YES = "YES", NO = "NO"
  ;

  private static String itoa(int val) {
    return Integer.toString(val);
  }

  private static int atoi(String val) {
    return Integer.parseInt(val);
  }

  private static String btoa(boolean b) {
    return Boolean.toString(b);
  }

  private static boolean atob(String val) { return Boolean.parseBoolean(val); }

  private static boolean matchesPattern(String value, String pattern) {
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

  /* SQLite does not support schemas or catalogs, treat catalog as database name */
  public static L4Result dbGetTables(String catalog, String schemaPattern,
                                     String tableNamePattern, String[] types,
                                     L4Client client) {
    var typeSet = types == null ? new HashSet<>(Arrays.asList(TABLE, VIEW)) : new HashSet<>(Arrays.asList(types));
    var typeFilter = "";
    if (!typeSet.contains(TABLE) && typeSet.contains(VIEW)) {
      typeFilter = " WHERE type = 'view'";
    } else if (typeSet.contains(TABLE) && !typeSet.contains(VIEW)) {
      typeFilter = " WHERE type = 'table'";
    } else {
      typeFilter = "WHERE (type = 'table' OR type = 'view')";
    }
    var sql = join("\n", "",
      "SELECT ",
      "  NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, name AS TABLE_NAME, ",
      "  type AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, ",
      "  NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION",
      "FROM sqlite_master",
      "%s",
      "AND name LIKE '%s'"
    );
    tableNamePattern = tableNamePattern == null ? "%" : quote(tableNamePattern);
    sql = format(sql, typeFilter, tableNamePattern);
    var response = client.querySingle(sql);
    var res = response.first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR
    );
    if (catalog != null && !catalog.isEmpty()) {
      res.values = res.values.stream()
        .filter(row -> matchesPattern(catalog, res.get(TABLE_CAT, row)))
        .collect(toList());
    }
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      res.values.clear();
    }
    return res;
  }

  public static L4Result dbGetCatalogs(L4Client client) {
    var rs = client.querySingle("PRAGMA database_list");
    var out = client.querySingle("SELECT * from (SELECT NULL TABLE_CAT) WHERE 1 = 0").first().setTypes(RQ_VARCHAR);
    var res0 = rs.first();
    res0.forEach((i, row) -> {
      var name = res0.get(kName, row);
      if (!name.equals(kTemp)) { // Exclude temporary database
        out.addRow(name);
      }
    });
    return out;
  }

  public static L4Result dbGetTableTypes(L4Client client) {
    var out = client
      .querySingle("SELECT * FROM (SELECT NULL TABLE_TYPE) WHERE 1 = 0")
      .first().setTypes(RQ_VARCHAR);
    return out.addRow(TABLE).addRow(VIEW);
  }

  public static L4Result dbGetColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern,
                                      L4Client client) {
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, ",
      "  NULL COLUMN_NAME, 0 AS DATA_TYPE, NULL TYPE_NAME, 0 AS COLUMN_SIZE, ",
      "  0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE, ",
      "  NULL REMARKS, NULL COLUMN_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, ",
      "  0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, NULL IS_NULLABLE, NULL SCOPE_CATALOG, ",
      "  NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, 0 AS SOURCE_DATA_TYPE, NULL IS_AUTOINCREMENT, ",
      "  NULL IS_GENERATEDCOLUMN",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_INTEGER, RQ_VARCHAR, RQ_INTEGER, RQ_INTEGER,
      RQ_INTEGER, RQ_INTEGER, RQ_INTEGER, RQ_VARCHAR,
      RQ_VARCHAR, RQ_INTEGER, RQ_INTEGER, RQ_INTEGER,
      RQ_INTEGER, RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_INTEGER, RQ_VARCHAR, RQ_VARCHAR
    );

    if (schemaPattern != null && !schemaPattern.isEmpty()) { // SQLite does not support schemas
      return out;
    }

    // Get all tables matching tableNamePattern
    var tables = dbGetTables(catalog, schemaPattern, tableNamePattern, new String[] {TABLE, VIEW}, client);

    tables.forEach((i, row) -> {
      var tableName = out.get(TABLE_NAME, row);
      var tableCatalog = out.get(TABLE_CAT, row);
      var ti = client.querySingle(format("PRAGMA table_info('%s')", quote(tableName)));
      var res0 = ti.first();

      res0.forEach((j, row0) -> {
        var ordinal = j + 1;
        var colName = res0.get(kName, row0);
        if (matchesPattern(colName, columnNamePattern)) {
          var type = res0.get(kType, row0);
          var notNull = res0.get(kNotNull, row0);
          var defaultValue = res0.get(kDfltValue, row0);
          var pk = res0.get(kPk, row0);
          var isAutoIncrement = pk != null
            && pk.equals(i1)
            && type.contains(RQ_INTEGER)
            && defaultValue != null
            && defaultValue.equalsIgnoreCase(kNull);
          var sqlType = getJdbcType(type);
          var columnSize = getJdbcTypePrecision(type);
          var decimalDigits = 0;

          out.addRow(tableCatalog,
            null, tableName, colName, itoa(sqlType), type,
            itoa(columnSize), itoa(0), itoa(decimalDigits), itoa(10),
            itoa( // NULLABLE
              notNull != null && notNull.equals(i1)
                ? DatabaseMetaData.columnNullable
                : DatabaseMetaData.columnNoNulls
            ),
            null, // REMARKS
            defaultValue, // COLUMN_DEF
            itoa(sqlType), // SQL_DATA_TYPE
            itoa(0), // SQL_DATETIME_SUB
            itoa(columnSize), // CHAR_OCTET_LENGTH
            itoa(ordinal), // ORDINAL_POSITION
            notNull != null && notNull.equals(i1) ? YES : NO, // IS_NULLABLE
            null, // SCOPE_CATALOG
            null, // SCOPE_SCHEMA
            null, // SCOPE_TABLE
            itoa(0), // SOURCE_DATA_TYPE
            isAutoIncrement ? YES : NO, // IS_AUTOINCREMENT
            NO // IS_GENERATEDCOLUMN
          );
        }
      });
    });

    return out;
  }

  public static L4Result dbGetPrimaryKeys(String catalog, String schema, String table, L4Client client) {
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, ",
      "         NULL AS TABLE_NAME, NULL AS COLUMN_NAME, 0 AS KEY_SEQ, NULL AS PK_NAME",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_INTEGER, RQ_VARCHAR
    );
    if (schema != null && !schema.isEmpty()) { // SQLite does not support schemas
      return out;
    }
    var tab = quote(table);
    var res = client.querySingle(format("PRAGMA table_info('%s')", tab)).first();
    final var keySeq = new int[] { 1 };
    res.forEach((i, row) -> {
      var pk = res.get(kPk, row);
      if (pk != null && !pk.equals(i0)) {
        out.addRow(
          catalog, null, tab, res.get(kName, row),
          itoa(keySeq[0]++), // TODO double check this.
          format("PK_%s", tab.toUpperCase())
        );
      }
    });
    return out;
  }

  public static L4Result dbGetBestRowIdentifier(String catalog, String schema, String table,
                                                boolean nullable, L4Client client) {
    // Use primary key columns as best row identifier
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT 0 AS SCOPE, NULL AS COLUMN_NAME, ",
      "         0 AS DATA_TYPE, NULL AS TYPE_NAME, 0 AS COLUMN_SIZE, ",
      "         0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS PSEUDO_COLUMN",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_INTEGER, RQ_VARCHAR, RQ_INTEGER,
      RQ_VARCHAR, RQ_INTEGER, RQ_INTEGER,
      RQ_INTEGER, RQ_INTEGER
    );
    var pkRs = dbGetPrimaryKeys(catalog, schema, table, client);
    pkRs.forEach((i, pkr) -> {
      var colName = pkRs.get(COLUMN_NAME, pkr);
      var colRs = dbGetColumns(catalog, schema, table, colName, client);
      colRs.forEach((j, cr) -> {
        var nFlag = colRs.get(NULLABLE, cr);
        var nfi = nFlag != null ? Integer.parseInt(nFlag) : -1;
        if (!nullable || nfi == DatabaseMetaData.columnNoNulls) {
          out.addRow(
            itoa(DatabaseMetaData.bestRowSession),
            colRs.get(COLUMN_NAME, cr), colRs.get(DATA_TYPE, cr),
            colRs.get(TYPE_NAME, cr), colRs.get(COLUMN_SIZE, cr),
            colRs.get(BUFFER_LENGTH, cr), colRs.get(DECIMAL_DIGITS, cr),
            itoa(DatabaseMetaData.bestRowNotPseudo)
          );
        }
      });
    });
    return out;
  }

  public static L4Result dbGetImportedKeys(String catalog, String table, L4Client client) {
    // SQLite does not support schemas
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, ",
      "         NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, ",
      "         NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, 0 AS KEY_SEQ, 0 AS UPDATE_RULE, ",
      "         0 AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_INTEGER, RQ_INTEGER,
      RQ_INTEGER, RQ_VARCHAR, RQ_VARCHAR, RQ_SMALLINT
    );
    var tab = quote(table);
    var rs = client.querySingle(format("PRAGMA foreign_key_list('%s')", tab)).first();
    rs.forEach((i, row) -> {
      var seq = Integer.toString(Integer.parseInt(rs.get(kSeq, row)) + 1);
      var from = rs.get(kFrom, row);
      var to = rs.get(kTo, row);
      var onDelete = rs.get(kOnDelete, row);
      out.addRow(
        catalog, null,
        rs.get(kTable, row), to,
        catalog, null, tab,
        from, seq,
        itoa(DatabaseMetaData.importedKeyNoAction), // UPDATE_RULE (SQLite does not support ON UPDATE)
        itoa(
          onDelete != null && onDelete.equals(CASCADE)
            ? DatabaseMetaData.importedKeyCascade
            : DatabaseMetaData.importedKeyNoAction
        ),
        format("FK_%s_%s", tab.toUpperCase(), from.toUpperCase()), // FK_NAME (synthetic)
        format("PK_%s", to.toUpperCase()),
        itoa(DatabaseMetaData.importedKeyNotDeferrable) // DEFERRABILITY
      );
    });
    return out;
  }

  public static L4Result dbGetExportedKeys(String catalog, String table, L4Client client) {
    // SQLite does not support schemas
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, ",
      "         NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, ",
      "         NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, 0 AS KEY_SEQ, 0 AS UPDATE_RULE, ",
      "         0 AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_SMALLINT,
      RQ_SMALLINT, RQ_SMALLINT, RQ_VARCHAR,
      RQ_VARCHAR, RQ_SMALLINT
    );
    // Find all tables with foreign keys referencing this table
    var tables = dbGetTables(catalog,null, null, new String[] { TABLE }, client);
    tables.forEach((i, row) -> {
      var fkTable = quote(tables.get(TABLE_NAME, row));
      var fkRs = client.querySingle(format("PRAGMA foreign_key_list('%s')", fkTable)).first();
      fkRs.forEach((j, row0) -> {
        if (table.equals(fkRs.get(kTable, row0))) {
          var seq = itoa(atoi(fkRs.get(kSeq, row0)) + 1);
          var from = fkRs.get(kFrom, row0);
          var to = fkRs.get(kTo, row0);
          var onDelete = fkRs.get(kOnDelete, row0);
          out.addRow(
            catalog, null, table,
            to, catalog, null, fkTable, from,
            seq, itoa(DatabaseMetaData.importedKeyNoAction),
            itoa(
              onDelete != null && onDelete.equals(CASCADE)
                ? DatabaseMetaData.importedKeyCascade
                : DatabaseMetaData.importedKeyNoAction
            ),
            format("FK_%s_%s", fkTable.toUpperCase(), from.toUpperCase()),
            format("PK_%s", to.toUpperCase()),
            itoa(DatabaseMetaData.importedKeyNotDeferrable)
          );
        }
      });
    });
    return out;
  }

  public static L4Result dbGetCrossReference(String parentCatalog, String parentTable,
                                             String foreignCatalog, String foreignTable,
                                             L4Client client) {
    // SQLite does not support schemas
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, NULL AS PKTABLE_NAME, ",
      "         NULL AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, ",
      "         NULL AS FKTABLE_NAME, NULL AS FKCOLUMN_NAME, ",
      "         0 AS KEY_SEQ, 0 AS UPDATE_RULE, 0 AS DELETE_RULE, ",
      "         NULL AS FK_NAME, NULL AS PK_NAME, 0 AS DEFERRABILITY",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_VARCHAR,
      RQ_SMALLINT, RQ_SMALLINT, RQ_SMALLINT,
      RQ_VARCHAR, RQ_VARCHAR, RQ_SMALLINT
    );
    var fkRs = dbGetImportedKeys(foreignCatalog, foreignTable, client);
    fkRs.forEach((i, row) -> {
      var pkTable = fkRs.get(PKTABLE_NAME, row);
      var pkTableCat = fkRs.get(PKTABLE_CAT, row);
      if (parentTable.equals(pkTable) && (parentCatalog == null || parentCatalog.equals(pkTableCat))) {
        out.addRow(
          pkTableCat, fkRs.get(PKTABLE_SCHEM, row), pkTable,
          fkRs.get(PKCOLUMN_NAME, row), fkRs.get(FKTABLE_CAT, row),
          fkRs.get(FKTABLE_SCHEM, row), fkRs.get(FKTABLE_NAME, row),
          fkRs.get(FKCOLUMN_NAME, row), fkRs.get(KEY_SEQ, row),
          fkRs.get(UPDATE_RULE, row), fkRs.get(DELETE_RULE, row),
          fkRs.get(FK_NAME, row), fkRs.get(PK_NAME, row),
          fkRs.get(DEFERRABILITY, row)
        );
      }
    });
    return out;
  }

  public static L4Result dbGetTypeInfo(L4Client client) {
    // Common SQLite types
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS TYPE_NAME, 0 AS DATA_TYPE, 0 AS PRECISION, ",
      "         NULL AS LITERAL_PREFIX, NULL AS LITERAL_SUFFIX, ",
      "         NULL AS CREATE_PARAMS, 0 AS NULLABLE, FALSE AS CASE_SENSITIVE, ",
      "         0 AS SEARCHABLE, FALSE AS UNSIGNED_ATTRIBUTE, FALSE AS FIXED_PREC_SCALE, ",
      "         FALSE AS AUTO_INCREMENT, NULL AS LOCAL_TYPE_NAME, 0 AS MINIMUM_SCALE, ",
      "         0 AS MAXIMUM_SCALE, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, ",
      "         0 AS NUM_PREC_RADIX",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_INTEGER, RQ_INTEGER,
      RQ_VARCHAR, RQ_VARCHAR,
      RQ_VARCHAR, RQ_SMALLINT, RQ_BOOLEAN,
      RQ_SMALLINT, RQ_BOOLEAN, RQ_BOOLEAN,
      RQ_BOOLEAN, RQ_VARCHAR, RQ_SMALLINT,
      RQ_SMALLINT, RQ_INTEGER, RQ_INTEGER,
      RQ_INTEGER
    );
    for (var rqt : RQ_TYPES) {
      var jt = getJdbcType(rqt);
      var hasPrefix = anyOf(jt, Types.VARCHAR, Types.DATE, Types.TIME, Types.TIMESTAMP, Types.DATALINK, Types.NVARCHAR);
      out.addRow(
        rqt, itoa(jt), itoa(getJdbcTypePrecision(rqt)),
        hasPrefix ? "'" : null, hasPrefix ? "'" : null,
        null, itoa(DatabaseMetaData.typeNullable), btoa(false),
        itoa(3), btoa(true), btoa(false),
        btoa(false), rqt, itoa(0),
        itoa(0), itoa(0), itoa(0),
        itoa(10)
      );
    }
    return out;
  }

  public static L4Result dbGetIndexInfo(String catalog, String table,
                                        boolean unique, L4Client client) {
    // SQLite does not support schemas
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NULL AS TABLE_NAME, ",
      "         FALSE AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, NULL AS INDEX_NAME, 0 AS TYPE, ",
      "         0 AS ORDINAL_POSITION, NULL AS COLUMN_NAME, NULL AS ASC_OR_DESC, 0 AS CARDINALITY, ",
      "         0 AS PAGES, NULL AS FILTER_CONDITION",
      ") WHERE 1 = 0"
    )).first().setTypes(
      RQ_VARCHAR, RQ_VARCHAR, RQ_VARCHAR,
      RQ_BOOLEAN, RQ_VARCHAR, RQ_VARCHAR, RQ_SMALLINT,
      RQ_INTEGER, RQ_VARCHAR, RQ_VARCHAR, RQ_BIGINT,
      RQ_BIGINT, RQ_VARCHAR
    );
    var rs = client.querySingle(format("PRAGMA index_list('%s')", quote(table))).first();
    rs.forEach((i, row) -> {
      var indexName = rs.get(kName, row);
      var isUnique = atoi(rs.get(kUnique, row)) == 1;
      var idxInfo = client.querySingle(format("PRAGMA index_info('%s')", quote(indexName))).first();
      var iexInfo = client.querySingle(format("PRAGMA index_xinfo('%s')", quote(indexName))).first();
      idxInfo.forEach((j, row0) -> {
        var skip = unique && !isUnique;
        if (!skip) {
          var colName = idxInfo.get(kName, row0);
          var colSort = new String[1];
          iexInfo.forEach((k, row1) -> {
            var colMatch = iexInfo.get(kName, row1);
            var desc = atob(iexInfo.get(kDesc, row1));
            if (colName.equals(colMatch)) {
              colSort[0] = desc ? "D" : "A";
            }
          });
          out.addRow(
            catalog, null, table,
            btoa(!isUnique), null, indexName, itoa(DatabaseMetaData.tableIndexOther),
            itoa(atoi(idxInfo.get(kSeqNo, row0)) + 1), colName, colSort[0], itoa(0),
            itoa(0), null
          );
        }
      });
    });
    return out;
  }

}
