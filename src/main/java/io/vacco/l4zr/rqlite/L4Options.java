package io.vacco.l4zr.rqlite;

import static java.lang.String.format;

public class L4Options {

  public static long    timeoutSec = 5;
  public static boolean transaction = true;
  public static boolean queue = false;
  public static boolean wait = true;
  public static L4Level level = L4Level.linearizable;
  public static long    linearizableTimeoutSec = 5;
  public static long    freshnessSec = 5;
  public static boolean freshness_strict = false;

  private static String kv(String key, Object value) {
    return String.format("%s=%s", key, value.toString());
  }

  public static String queryParams() {
    var pairs = new String[] {
      transaction ? kv("transaction", true) : "",
      kv("timings", true),
      kv("timeout", format("%ds", timeoutSec)),
      queue ? kv("queue", queue) : "",
      kv("wait", wait),
      kv("level", level),
      level == L4Level.linearizable
        ? kv("linearizable_timeout", format("%ds", linearizableTimeoutSec))
        : "",
      kv("freshness", format("%ds", freshnessSec)),
      kv("freshness_strict", freshness_strict)
    };
    var params = String.join("&", pairs);
    return String.format("?%s", params);
  }

  // TODO implement parameter parsing from JDBC URI string

}
