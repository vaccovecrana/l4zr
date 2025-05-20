package io.vacco.l4zr.jdbc;

import io.vacco.l4zr.rqlite.*;
import javax.net.ssl.SSLContext;
import java.net.http.HttpClient;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

import static io.vacco.l4zr.jdbc.L4Err.*;

public class L4Driver implements Driver {
  private static final String JDBC_URL_PREFIX = "jdbc:rqlite:";
  private static final Logger LOGGER = Logger.getLogger(L4Driver.class.getName());

  static {
    try {
      DriverManager.registerDriver(new L4Driver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register L4Driver", e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null; // Return null if the URL is not for this driver
    }

    try {
      // Parse the JDBC URL and set L4Options
      ParsedUrl parsedUrl = parseRqliteUrl(url);
      Properties mergedProps = mergeProperties(info, parsedUrl.queryParams);

      // Create an HttpClient based on the protocol and query parameters
      HttpClient httpClient = createHttpClient(parsedUrl.baseUrl, parsedUrl.queryParams);

      // Create an L4Client instance
      L4Client client = createClient(parsedUrl.baseUrl, mergedProps, httpClient);

      // Return a new L4Connection
      return new L4Connection(client);
    } catch (Exception e) {
      throw new SQLException("Failed to establish connection: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url == null) {
      return false;
    }
    return url.toLowerCase().startsWith(JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    // Define properties that can be specified in the URL or Properties
    Properties mergedProps = mergeProperties(info, new HashMap<>());
    DriverPropertyInfo[] props = new DriverPropertyInfo[11];

    props[0] = new DriverPropertyInfo("user", mergedProps.getProperty("user"));
    props[0].description = "Username for rqlite authentication";
    props[0].required = false;

    props[1] = new DriverPropertyInfo("password", mergedProps.getProperty("password"));
    props[1].description = "Password for rqlite authentication";
    props[1].required = false;

    props[2] = new DriverPropertyInfo("timeout", mergedProps.getProperty("timeout", String.valueOf(L4Options.timeoutSec)));
    props[2].description = "Timeout in seconds";
    props[2].required = false;

    props[3] = new DriverPropertyInfo("transaction", mergedProps.getProperty("transaction", String.valueOf(L4Options.transaction)));
    props[3].description = "Enable transaction mode";
    props[3].required = false;

    props[4] = new DriverPropertyInfo("queue", mergedProps.getProperty("queue", String.valueOf(L4Options.queue)));
    props[4].description = "Enable queue mode";
    props[4].required = false;

    props[5] = new DriverPropertyInfo("wait", mergedProps.getProperty("wait", String.valueOf(L4Options.wait)));
    props[5].description = "Enable wait mode";
    props[5].required = false;

    props[6] = new DriverPropertyInfo("level", mergedProps.getProperty("level", L4Options.level.toString()));
    props[6].description = "Consistency level (none, weak, linearizable)";
    props[6].required = false;

    props[7] = new DriverPropertyInfo("linearizable_timeout", mergedProps.getProperty("linearizable_timeout", String.valueOf(L4Options.linearizableTimeoutSec)));
    props[7].description = "Linearizable timeout in seconds";
    props[7].required = false;

    props[8] = new DriverPropertyInfo("freshness", mergedProps.getProperty("freshness", String.valueOf(L4Options.freshnessSec)));
    props[8].description = "Freshness in seconds";
    props[8].required = false;

    props[9] = new DriverPropertyInfo("freshness_strict", mergedProps.getProperty("freshness_strict", String.valueOf(L4Options.freshness_strict)));
    props[9].description = "Enable strict freshness";
    props[9].required = false;

    props[10] = new DriverPropertyInfo("cacert", mergedProps.getProperty("cacert"));
    props[10].description = "Path to CA certificate for HTTPS connections";
    props[10].required = false;

    return props;
  }

  @Override
  public int getMajorVersion() {
    return 1; // Define major version
  }

  @Override
  public int getMinorVersion() {
    return 0; // Define minor version
  }

  @Override
  public boolean jdbcCompliant() {
    return false; // Set to true if fully JDBC compliant, false for partial compliance
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return LOGGER;
  }

  private static class ParsedUrl {
    final String baseUrl;
    final Map<String, String> queryParams;

    ParsedUrl(String baseUrl, Map<String, String> queryParams) {
      this.baseUrl = baseUrl;
      this.queryParams = queryParams;
    }
  }

  private ParsedUrl parseRqliteUrl(String url) throws SQLException {
    if (url == null || !url.toLowerCase().startsWith(JDBC_URL_PREFIX)) {
      throw new SQLException("Invalid rqlite JDBC URL: " + url);
    }

    // Extract the base URL and query parameters
    String rqliteUrl = url.substring(JDBC_URL_PREFIX.length());
    String[] urlParts = rqliteUrl.split("\\?", 2);
    String baseUrl = urlParts[0];
    Map<String, String> queryParams = new HashMap<>();

    if (urlParts.length > 1) {
      String[] params = urlParts[1].split("&");
      for (String param : params) {
        String[] keyValue = param.split("=", 2);
        if (keyValue.length == 2) {
          queryParams.put(keyValue[0].toLowerCase(), keyValue[1]);
        }
      }
    }

    updateL4Options(queryParams);
    return new ParsedUrl(baseUrl, queryParams);
  }

  private void updateL4Options(Map<String, String> queryParams) throws SQLException {
    try {
      if (queryParams.containsKey("timeout")) {
        L4Options.timeoutSec = Long.parseLong(queryParams.get("timeout").replace("s", ""));
      }
      if (queryParams.containsKey("transaction")) {
        L4Options.transaction = Boolean.parseBoolean(queryParams.get("transaction"));
      }
      if (queryParams.containsKey("queue")) {
        L4Options.queue = Boolean.parseBoolean(queryParams.get("queue"));
      }
      if (queryParams.containsKey("wait")) {
        L4Options.wait = Boolean.parseBoolean(queryParams.get("wait"));
      }
      if (queryParams.containsKey("level")) {
        String level = queryParams.get("level").toLowerCase();
        try {
          L4Options.level = L4Level.valueOf(level);
        } catch (IllegalArgumentException e) {
          throw new SQLException("Invalid consistency level: " + level, e);
        }
      }
      if (queryParams.containsKey("linearizable_timeout")) {
        L4Options.linearizableTimeoutSec = Long.parseLong(queryParams.get("linearizable_timeout").replace("s", ""));
      }
      if (queryParams.containsKey("freshness")) {
        L4Options.freshnessSec = Long.parseLong(queryParams.get("freshness").replace("s", ""));
      }
      if (queryParams.containsKey("freshness_strict")) {
        L4Options.freshness_strict = Boolean.parseBoolean(queryParams.get("freshness_strict"));
      }
    } catch (Exception e) {
      throw badParam(e);
    }
  }

  private Properties mergeProperties(Properties info, Map<String, String> queryParams) {
    Properties merged = new Properties();
    // Add query parameters first
    queryParams.forEach(merged::setProperty);
    // Override with info properties if provided
    if (info != null) {
      merged.putAll(info);
    }
    return merged;
  }

  private HttpClient createHttpClient(String baseUrl, Map<String, String> queryParams) throws SQLException {
    try {
      var isHttps = baseUrl.toLowerCase().startsWith("https://");
      var cacert = queryParams.get("cacert");
      var insecure = queryParams.get("insecure");
      if (!isHttps) {
        return L4Http.defaultHttpClient();
      } else if ("true".equalsIgnoreCase(insecure)) {
        return L4Http.newTLSSClientInsecure();
      } else if (cacert != null && !cacert.isEmpty()) {
        return L4Http.newTLSSClient(cacert);
      } else {
        return HttpClient.newBuilder()
            .sslContext(SSLContext.getDefault()) // Default HTTPS with system trust store
            .connectTimeout(Duration.ofSeconds(L4Options.timeoutSec))
            .build();
      }
    } catch (Exception e) {
      throw badParam(e);
    }
  }

  private L4Client createClient(String baseUrl, Properties props, HttpClient httpClient) throws SQLException {
    try {
      String user = props.getProperty("user");
      String password = props.getProperty("password");
      // Instantiate L4Client with baseUrl, credentials, and HttpClient
      // Adjust based on actual L4Client constructor
      return new L4Client(baseUrl, httpClient).withBasicAuth(); // Placeholder: replace with actual L4Client creation
    } catch (Exception e) {
      throw new SQLException("Failed to create L4Client: " + e.getMessage(), e);
    }
  }

}