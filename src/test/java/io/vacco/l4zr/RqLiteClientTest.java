package io.vacco.l4zr;

import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import rqlite.RqliteClient;
import rqlite.sql.SQLStatements;
import rqlite.util.HttpClients;

import static java.lang.String.join;
import static rqlite.sql.SQLStatement.*;
import static j8spec.J8Spec.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class RqLiteClientTest {
  static {
    it("Interacts with an Rqlite instance", () -> {
      var rq = new RqliteClient("http://localhost:4001", HttpClients.defaultHttpClient());
      var status = rq.status();
      var nodes = rq.nodes();

      var res0 = rq.executeSingle(join("\n", "",
        "CREATE TABLE IF NOT EXISTS users (",
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "  name TEXT NOT NULL,",
        "  email TEXT NOT NULL UNIQUE,",
        "  age INTEGER",
        ")"
      ));

      var res1 = rq.querySingle("SELECT * FROM users");
      System.out.println(res1);

      if (res1.results != null) {
        var rl = res1.results;
        var vals = rl.get(0).values;
        if (vals == null || vals.isEmpty()) {
          var statements = new SQLStatements();
          statements.add(newSQLStatement("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)"));
          statements.add(newSQLStatement("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)"));
          statements.add(newSQLStatement("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)"));
          var res2 = rq.execute(statements);
          System.out.println(res2);
        }
      }

      var res3 = rq.querySingle("SELECT * FROM users WHERE age > ?", 30);
      System.out.println(res3);
    });
  }
}
