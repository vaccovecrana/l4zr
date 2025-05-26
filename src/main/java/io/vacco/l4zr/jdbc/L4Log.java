package io.vacco.l4zr.jdbc;

import java.util.function.BiConsumer;

public class L4Log {

  public static BiConsumer<String, Object[]> traceFn;

  public static void l4Trace(String format, Object ... args) {
    if (traceFn != null) {
      traceFn.accept(format, args);
    }
  }

}
