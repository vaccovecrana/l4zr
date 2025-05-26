package io.vacco.l4zr;

import io.vacco.l4zr.rqlite.L4Client;
import io.vacco.l4zr.rqlite.L4Http;
import io.vacco.l4zr.rqlite.L4Options;

import static org.junit.Assert.assertEquals;

public class L4Tests {

  public static L4Client localClient() {
    return new L4Client("http://localhost:4001", L4Http.defaultHttpClient(L4Options.timeoutSec).build());
  }

  public static void setupPreparedStatementTestTable(L4Client rq) {
    var dr = rq.executeSingle("DROP TABLE IF EXISTS ps_test_data");
    assertEquals(200, dr.statusCode);

    var createTable = String.join("\n", "",
      "CREATE TABLE ps_test_data (",
      "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
      "  num_val NUMERIC,",
      "  bool_val BOOLEAN,",
      "  tiny_val TINYINT,",
      "  small_val SMALLINT,",
      "  int_val INTEGER,",
      "  big_val BIGINT,",
      "  float_val FLOAT,",
      "  double_val DOUBLE,",
      "  text_val VARCHAR,",
      "  date_val DATE,",
      "  time_val TIME,",
      "  ts_val TIMESTAMP,",
      "  url_val DATALINK,",
      "  clob_val CLOB,",
      "  nclob_val NCLOB,",
      "  nstring_val NVARCHAR,",
      "  blob_val BLOB",
      ")"
    );
    var res0 = rq.executeSingle(createTable);
    assertEquals(200, res0.statusCode);
  }

}
