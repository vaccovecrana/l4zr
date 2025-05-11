package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import java.util.*;
import java.util.regex.Pattern;

import static java.lang.String.*;
import static java.util.stream.Collectors.toList;

public class L4Db {

  public static final String
    TABLE = "TABLE", VIEW = "VIEW",
    TABLE_CAT = "TABLE_CAT";

  private final L4Client client;

  public L4Db(L4Client client) {
    this.client = Objects.requireNonNull(client);
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

  /* SQLite does not support schemas or catalogs, treat catalog as database name */
  public L4Result getTables(String catalog, String tableNamePattern,
                            String schemaPattern, String[] types) {
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
    var response = client.query(new L4Statement().sql(sql));
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

}
