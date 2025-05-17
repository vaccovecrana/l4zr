l4zr

## Caveats

Right now, result sets returned from `rqlite` HTTP calls will be held in memory,
(i.e. mapped from raw JSON data to JDBC Result sets). So in general, try to write
queries that bring back small amounts of data.

```
Final Implementation Order
L4Exception: Foundational error handling.
L4Driver: Entry point for JDBC.
L4Connection: Manages connections and statements.
L4Statement: Executes SQL queries.
L4ResultSet: Processes query results.
L4ResultSetMetaData: Provides result set metadata.
```

## Driver Options and Defaults

Here’s a list of options to expose via the JDBC URL, along with defaults that support clustered environments and align with JDBC expectations:

timeout (Duration):
Description: Timeout for both /db/execute and /db/query requests.
Default: 10s (10 seconds).
Reason: Prevents requests from hanging indefinitely in a clustered setup where nodes might be slow to respond.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?timeout=5s

queue (boolean):
Description: Queues requests if the node isn’t the leader (for /db/execute).
Default: true.
Reason: In a clustered environment, this ensures write requests are handled correctly even if the contacted node isn’t the leader, improving reliability.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?queue=false

wait (boolean):
Description: Waits for write requests to be applied to the Raft log (for /db/execute).
Default: true.
Reason: Ensures strong consistency for writes in a clustered setup, which is what JDBC users expect.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?wait=false

level (String: none, weak, strong, linearizable):
Description: Read consistency level for /db/query.
Default: strong.
Reason: JDBC users expect consistent reads. strong ensures the node has the latest committed data without the overhead of linearizable, making it a good default for clustered environments.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?level=linearizable

linearizable_timeout (Duration):
Description: Timeout for linearizable reads (used when level=linearizable).
Default: Same as timeout (10 seconds).
Reason: Prevents linearizable reads from hanging indefinitely in a clustered setup.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?linearizable_timeout=5s

freshness (Duration):
Description: Maximum age of data for weak or none reads.
Default: None (not set).
Reason: Only relevant if level=weak or none. Users can set this in clustered environments to ensure data freshness.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?level=weak&freshness=1s

freshness_strict (boolean):
Description: Enforces strict freshness for weak or none reads.
Default: false.
Reason: Strict freshness can cause requests to fail if the data isn’t fresh enough, so it’s better to leave it off by default.
JDBC URL Example: jdbc:l4zr:http://localhost:4001/mydb?level=weak&freshness=1s&freshness_strict=true

The rqlite JDBC driver does not support setAutoCommit, commit, or rollback, as rqlite executes all statements atomically using transaction=true. To perform transactions (atomic execution of multiple statements), use the batch methods addBatch and executeBatch in Statement or PreparedStatement.

For example:java

Statement stmt = conn.createStatement();
stmt.addBatch("INSERT INTO foo(name) VALUES('fiona')");
stmt.addBatch("INSERT INTO foo(name) VALUES('sinead')");
int[] updateCounts = stmt.executeBatch(); // Executes atomically

For prepared statements:

PreparedStatement ps = conn.prepareStatement("INSERT INTO foo(name) VALUES(?)");
ps.setString(1, "fiona");
ps.addBatch();
ps.setString(1, "sinead");
ps.addBatch();
int[] updateCounts = ps.executeBatch(); // Executes atomically

The driver supports only TRANSACTION_SERIALIZABLE isolation, with linearizable read consistency by default, which is suitable for most production use cases, including transactions via batching. If you override the read consistency level (e.g., jdbc:rqlite://host:port/db?level=weak), you are responsible for handling any read inconsistencies that may occur in your transactions. For stricter consistency, set level=strong in the JDBC URL.

The rqlite JDBC driver supports getTypeMap and setTypeMap for JDBC compliance, but these methods have no effect, as rqlite (based on SQLite) does not support user-defined SQL types (UDTs). Data is mapped to standard Java types (e.g., String, Integer, byte[]).
