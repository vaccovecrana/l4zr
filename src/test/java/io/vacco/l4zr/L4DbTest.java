package io.vacco.l4zr;

import io.vacco.l4zr.jdbc.L4Db;
import io.vacco.l4zr.rqlite.L4Client;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import java.awt.GraphicsEnvironment;

import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4DbTest {

  private static final L4Client rq = L4Tests.localClient();

  static {
    if (!GraphicsEnvironment.isHeadless()) {
      it("Retrieves basic DB metadata", () -> {
        var db = new L4Db(rq);
        var res = db.getTables(null, null, null);
        res.print(System.out);
      });
    }
  }
}
