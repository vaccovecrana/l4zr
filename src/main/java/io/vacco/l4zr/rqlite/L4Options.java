package io.vacco.l4zr.rqlite;

public class L4Options {

  public static String timeout = "5s";
  public static boolean queue = true;
  public static boolean wait = true;
  public static L4Level level = L4Level.strong;
  public static String linearizable_timeout = "5s";
  public static String freshness = "1s";
  public static boolean freshness_strict = false;

  private static String kv(String key, Object value) {
    return String.format("%s=%s", key, value.toString());
  }

  public static String queryParams() {
    return String.format("?%s&%s&%s&%s&%s&%s&%s&%s&%s",
      kv("transaction", true),
      kv("timings", true),
      kv("timeout", timeout),
      kv("queue", queue),
      kv("wait", wait),
      kv("level", level),
      kv("linearizable_timeout", linearizable_timeout),
      kv("freshness", freshness),
      kv("freshness_strict", freshness_strict)
    );
  }

}
