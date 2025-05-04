package io.vacco.l4zr;

public class L4StatementTest {

  /* TODO add
  // Simple statement with no parameters
L4SqlStatementBuilder builder1 = new L4SqlStatementBuilder()
    .sql("SELECT * FROM users");
JsonArray statement1 = builder1.build();
System.out.println(statement1.toString());
// Output: ["SELECT * FROM users"]

// Statement with positional parameters
L4SqlStatementBuilder builder2 = new L4SqlStatementBuilder()
    .sql("SELECT * FROM users WHERE id = ? AND name = ?")
    .withPositionalParam(1)
    .withPositionalParam("Alice");
JsonArray statement2 = builder2.build();
System.out.println(statement2.toString());
// Output: ["SELECT * FROM users WHERE id = ? AND name = ?", 1, "Alice"]

// Statement with named parameters
L4SqlStatementBuilder builder3 = new L4SqlStatementBuilder()
    .sql("SELECT * FROM users WHERE id = :id AND name = :name")
    .withNamedParam("id", 1)
    .withNamedParam("name", "Alice");
JsonArray statement3 = builder3.build();
System.out.println(statement3.toString());
// Output: ["SELECT * FROM users WHERE id = :id AND name = :name", {"id": 1, "name": "Alice"}]

// Statement with a BLOB parameter
byte[] blobData = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
L4SqlStatementBuilder builder4 = new L4SqlStatementBuilder()
    .sql("INSERT INTO users (id, data) VALUES (?, ?)")
    .withPositionalParam(1)
    .withPositionalParam(blobData);
JsonArray statement4 = builder4.build();
System.out.println(statement4.toString());
// Output: ["INSERT INTO users (id, data) VALUES (?, ?)", 1, "3q2+7w=="]
   */

}
