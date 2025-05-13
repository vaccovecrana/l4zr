package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.sql.DatabaseMetaData;
import java.util.*;
import java.util.regex.Pattern;

import static io.vacco.l4zr.jdbc.L4Jdbc.*;
import static java.lang.String.*;
import static java.util.stream.Collectors.toList;

public class L4Db {

  public static final String
    TABLE = "TABLE", VIEW = "VIEW",
    TABLE_CAT = "TABLE_CAT", TABLE_NAME = "TABLE_NAME",
    kName = "name", kType = "type", kNotNull = "notnull",
    kDfltValue = "dflt_value", kPk = "pk", kNull = "null"
  ;

  private static String atoi(int val) {
    return Integer.toString(val);
  }

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
  public static L4Result dbGetTables(String catalog, String tableNamePattern,
                                     String schemaPattern, String[] types, L4Client client) {
    var typeSet = types == null ? new HashSet<>(Arrays.asList(TABLE, VIEW)) : new HashSet<>(Arrays.asList(types));
    var typeFilter = "";
    if (!typeSet.contains(TABLE) && typeSet.contains(VIEW)) {
      typeFilter = " WHERE type = 'view'";
    } else if (typeSet.contains(TABLE) && !typeSet.contains(VIEW)) {
      typeFilter = " WHERE type = 'table'";
    }
    var sql = join("\n", "",
      "SELECT ",
      " NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, name AS TABLE_NAME, ",
      " type AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, ",
      " NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION ",
      "FROM sqlite_master%s ",
      "WHERE name LIKE '%s' AND (type = 'table' OR type = 'view')"
    );
    tableNamePattern = tableNamePattern == null ? "%" : tableNamePattern.replace("'", "''");
    sql = format(sql, typeFilter, tableNamePattern);
    var response = client.querySingle(sql);
    var res = response.results.get(0);
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
    var out = client.querySingle("SELECT * from (SELECT '' AS TABLE_CAT) WHERE 1 = 0");
    var res0 = rs.results.get(0);
    var out0 = out.results.get(0);
    res0.forEach((i, row) -> {
      var name = res0.get("name", row);
      if (!name.equals("temp")) { // Exclude temporary database
        out0.addRow(name);
      }
    });
    return out0;
  }

  public static L4Result dbGetTableTypes(L4Client client) {
    var out = client.querySingle("Select * from (SELECT '' as TABLE_TYPE) WHERE 1 = 0").results.get(0);
    return out.addRow(TABLE).addRow(VIEW);
  }

  public static L4Result dbGetColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern, L4Client client) {
    var out = client.querySingle(join("\n", "",
      "SELECT * FROM (",
      "  SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, ",
      "  '' AS COLUMN_NAME, 0 AS DATA_TYPE, '' AS TYPE_NAME, 0 AS COLUMN_SIZE, ",
      "  0 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE, ",
      "  '' AS REMARKS, '' AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, 0 AS SQL_DATETIME_SUB, ",
      "  0 AS CHAR_OCTET_LENGTH, 0 AS ORDINAL_POSITION, '' AS IS_NULLABLE, '' AS SCOPE_CATALOG, ",
      "  '' AS SCOPE_SCHEMA, '' AS SCOPE_TABLE, 0 AS SOURCE_DATA_TYPE, '' AS IS_AUTOINCREMENT, ",
      "  '' AS IS_GENERATEDCOLUMN",
      ") WHERE 1 = 0"
    )).results.get(0);

    out.types.set(0, RQ_VARCHAR);
    out.types.set(1, RQ_VARCHAR);
    out.types.set(2, RQ_VARCHAR);
    out.types.set(3, RQ_VARCHAR);
    out.types.set(4, RQ_INTEGER);
    out.types.set(5, RQ_VARCHAR);
    out.types.set(6, RQ_INTEGER);
    out.types.set(7, RQ_INTEGER);
    out.types.set(8, RQ_INTEGER);
    out.types.set(9, RQ_INTEGER);
    out.types.set(10, RQ_INTEGER);
    out.types.set(11, RQ_VARCHAR);
    out.types.set(12, RQ_VARCHAR);
    out.types.set(13, RQ_INTEGER);
    out.types.set(14, RQ_INTEGER);
    out.types.set(15, RQ_INTEGER);
    out.types.set(16, RQ_INTEGER);
    out.types.set(17, RQ_VARCHAR);
    out.types.set(18, RQ_VARCHAR);
    out.types.set(19, RQ_VARCHAR);
    out.types.set(20, RQ_VARCHAR);
    out.types.set(21, RQ_INTEGER);
    out.types.set(22, RQ_VARCHAR);
    out.types.set(23, RQ_VARCHAR);

    if (schemaPattern != null && !schemaPattern.isEmpty()) { // SQLite does not support schemas
      return out;
    }

    // Get all tables matching tableNamePattern
    var tables = dbGetTables(catalog, null, tableNamePattern, new String[] {TABLE, VIEW}, client);

    tables.forEach((i, row) -> {
      var tableName = out.get(TABLE_NAME, row);
      var tableCatalog = out.get(TABLE_CAT, row);
      var ti = client.querySingle(format("PRAGMA table_info('%s')", tableName.replace("'", "''")));
      var res0 = ti.results.get(0);

      res0.forEach((j, row0) -> {
        var ordinal = j + 1;
        var colName = res0.get(kName, row0);
        if (matchesPattern(colName, columnNamePattern)) {
          var type = res0.get(kType, row0);
          var notNull = res0.get(kNotNull, row0);
          var defaultValue = res0.get(kDfltValue, row0);
          var pk = res0.get(kPk, row0);
          var isAutoIncrement = pk != null
            && pk.equals("1")
            && type.contains("INTEGER")
            && defaultValue != null
            && defaultValue.equalsIgnoreCase(kNull);
          var sqlType = getJdbcType(type);
          var columnSize = getJdbcTypePrecision(type);
          var decimalDigits = 0;

          out.addRow(tableCatalog,
            null, tableName, colName, atoi(sqlType), type,
            atoi(columnSize), atoi(0), atoi(decimalDigits), atoi(10),
            atoi( // NULLABLE
              notNull != null && notNull.equals("1")
                ? DatabaseMetaData.columnNullable
                : DatabaseMetaData.columnNoNulls
            ),
            null, // REMARKS
            defaultValue, // COLUMN_DEF
            atoi(sqlType), // SQL_DATA_TYPE
            atoi(0), // SQL_DATETIME_SUB
            atoi(columnSize), // CHAR_OCTET_LENGTH
            atoi(ordinal), // ORDINAL_POSITION
            notNull != null && notNull.equals("1") ? "YES" : "NO", // IS_NULLABLE
            null, // SCOPE_CATALOG
            null, // SCOPE_SCHEMA
            null, // SCOPE_TABLE
            atoi(0), // SOURCE_DATA_TYPE
            isAutoIncrement ? "YES" : "NO", // IS_AUTOINCREMENT
            "NO" // IS_GENERATEDCOLUMN
          );
        }
      });
    });

    return out;
  }

}
