package io.vacco.l4zr.jdbc;

import javax.sql.rowset.serial.SerialClob;
import java.sql.NClob;
import java.sql.SQLException;

public class L4NClob extends SerialClob implements NClob {
  public L4NClob(char[] data) throws SQLException {
    super(data);
  }
}
