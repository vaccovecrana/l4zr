package io.vacco.l4zr;

import io.vacco.l4zr.rqlite.L4Statement;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import io.vacco.l4zr.rqlite.*;
import java.awt.GraphicsEnvironment;

import static java.lang.String.join;
import static j8spec.J8Spec.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4ClientTest {
  static {
    if (!GraphicsEnvironment.isHeadless()) {
      it("Interacts with an Rqlite instance", () -> {
        var rq = new L4Client("http://localhost:4001", L4Http.defaultHttpClient());

        System.out.println(rq.status().toString());
        System.out.println(rq.nodes().toString());
        System.out.println(rq.ready());

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
            var res2 = rq.execute(
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)"),
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)"),
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")
            );
            System.out.println(res2);
          }
        }

        var res3 = rq.querySingle("SELECT * FROM users WHERE age > ?", 30);
        System.out.println(res3);
      });
    }
  }
}
