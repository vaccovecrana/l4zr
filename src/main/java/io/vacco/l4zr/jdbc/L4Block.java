package io.vacco.l4zr.jdbc;

@FunctionalInterface
public interface L4Block {
  void tryRun() throws Exception;
}
