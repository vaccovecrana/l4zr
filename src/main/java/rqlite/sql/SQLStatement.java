package rqlite.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/* SQL statement types. */
public class SQLStatement {
  public String sql;
  public List<Object> positionalParams;
  public Map<String, Object> namedParams;

  public SQLStatement() {
  }

  public SQLStatement(String sql) {
    this.sql = sql;
  }

  public static SQLStatement newSQLStatement(String stmt, Object... args) {
    SQLStatement s = new SQLStatement(stmt);
    if (args.length == 0) {
      return s;
    }
    if (args.length == 1 && args[0] instanceof Map) {
      s.namedParams = (Map<String, Object>) args[0];
    } else {
      s.positionalParams = Arrays.asList(args);
    }
    return s;
  }
}
