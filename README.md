# l4zr - rqlite JDBC Driver

`l4zr` is a minimal (~130KB, zero dependencies),  Type 4 JDBC driver for [rqlite](https://github.com/rqlite/rqlite),
a lightweight, distributed relational database built on top of SQLite.

This driver enables Java applications to interact with `rqlite` over HTTP, supporting standard
JDBC operations like queries, updates, and batch processing in a clustered environment.

## Features

- **JDBC Compliance**: Supports core JDBC APIs, including `Connection`, `Statement`, `PreparedStatement`, and `ResultSet`.
- **Atomic Transactions**: Executes multiple statements atomically using rqlite’s `transaction=true` mode via batch operations.
- **Clustered Environment Support**: Configurable options for read consistency, write queuing, and timeouts to handle rqlite’s distributed nature.
- **Schema and Metadata Access**: Query table metadata, primary keys, foreign keys, and indexes (see [L4DriverTest](./src/test/java/io/vacco/l4zr/L4DriverTest.java)).

## Getting Started

### Prerequisites

- Java 11 or higher
- `rqlite` server running (e.g., `http://localhost:4001`)

Install from [Maven Central](https://mvnrepository.com/artifact/io.vacco.l4zr/l4zr)

    io.vacco.l4zr:l4zr:[version]

The driver version corresponds to the last known `rqlite` [release](https://github.com/rqlite/rqlite/releases) the driver was tested against.

## Basic Usage

Connect to an `rqlite` instance and execute queries using standard JDBC APIs.

```
import java.sql.*;

var url = "jdbc:sqlite:http://localhost:4001";

try (Connection conn = DriverManager.getConnection(url)) {
    var stmt = conn.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    var ps = conn.prepareStatement("INSERT INTO users (name, age) VALUES (?, ?)");
    ps.setString(1, "Alice");
    ps.setInt(2, 30);
    ps.executeUpdate();

    var rs = stmt.executeQuery("SELECT * FROM users");
    while (rs.next()) {
        System.out.println("ID: " + rs.getInt("id") + ", Name: " + rs.getString("name") + ", Age: " + rs.getInt("age"));
    }
}
```

## Batch Processing for Transactions

`rqlite` executes statements atomically with `transaction=true`. Use batch operations for multi-statement transactions.

Using `Statement`:

```
Statement stmt = conn.createStatement();
stmt.addBatch("INSERT INTO users (name, age) VALUES ('Fiona', 25)");
stmt.addBatch("INSERT INTO users (name, age) VALUES ('Sinead', 28)");
int[] updateCounts = stmt.executeBatch(); // Executes atomically
```

Using `PreparedStatement`:

```
PreparedStatement ps = conn.prepareStatement("INSERT INTO users (name, age) VALUES (?, ?)");
ps.setString(1, "Fiona");
ps.setInt(2, 25);
ps.addBatch();
ps.setString(1, "Sinead");
ps.setInt(2, 28);
ps.addBatch();
int[] updateCounts = ps.executeBatch(); // Executes atomically
```

See [L4PsTest](./src/test/java/io/vacco/l4zr/L4PsTest.java) for advanced examples with various data types, streams, and LOBs.

## Configuration Options

Customize the driver’s behavior via JDBC URL parameters. Below are the available options, their defaults, and their purposes.

These options come from `rqlite`'s [Developer Guide](https://rqlite.io/docs/api)

| Option                | Description                                                                 | Default           | Example URL Parameter                     |
|-----------------------|-----------------------------------------------------------------------------|-------------------|-------------------------------------------|
| `timeout`             | Timeout for `/db/execute` and `/db/query` requests (e.g., `5s`, `500ms`).   | `5s`              | `?timeout=5s`                             |
| `queue`               | Queues write requests if the contacted node isn’t the leader.               | `true`            | `?queue=false`                            |
| `wait`                | Waits for write requests to be applied to the Raft log.                     | `true`            | `?wait=false`                             |
| `level`               | Read consistency level: `none`, `weak`, `strong`, `linearizable`.           | `linearizable`    | `?level=linearizable`                     |
| `linearizable_timeout`| Timeout for linearizable reads (when `level=linearizable`).                 | Same as `timeout` | `?linearizable_timeout=5s`                |
| `freshness`           | Maximum data age for `weak` or `none` reads (e.g., `1s`, `500ms`).          | None              | `?level=weak&freshness=1s`                |
| `freshness_strict`    | Enforces strict freshness for `weak` or `none` reads (fails if not fresh).  | `false`           | `?level=weak&freshness=1s&freshness_strict=true` |

Example JDBC URL:

```java
String url = "jdbc:sqlite:http://localhost:4001/mydb?timeout=5s&level=strong&freshness=1s";
```

## Caveats

- Memory Usage: Result sets are held in memory (mapped from rqlite’s JSON responses to JDBC ResultSet). Write queries that return small datasets to avoid memory issues.
- Catalog Support: Only the `main` SQLite database is reported as a catalog to JDBC.
- Transaction Limitations: The driver does nothing when calling `setAutoCommit`, `commit`, or `rollback` due to `rqlite`'s [Transaction support](https://rqlite.io/docs/api/api/#transactions). Use batch operations for atomic multi-statement execution.
- Isolation Level: Only `TRANSACTION_SERIALIZABLE` is supported, with `linearizable` read consistency by default. Setting `level=weak` or `level=none` may introduce read inconsistencies.
- Type Mapping: User-defined SQL types (UDTs) are not supported. getTypeMap and setTypeMap are implemented for compliance but have no effect.

## Contributing

Contributions are welcome! Please submit issues or pull requests to this GitHub repository.

Requires Gradle 8.1 or later.

Create a file with the following content at `~/.gsOrgConfig.json`:

```
{
  "orgId": "vacco-oss",
  "orgConfigUrl": "http://56db25d3f6c39937b48e-6eaf716421c53330be45fa9d36560381.r85.cf2.rackcdn.com/org-config/vacco.json"
}
```

Then run:

```
gradle clean build
```
