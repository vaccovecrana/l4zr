package rqlite;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.io.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.vacco.l4zr.jdbc.L4Options;
import io.vacco.l4zr.jdbc.L4Response;
import io.vacco.l4zr.json.Json;
import rqlite.sql.SQLStatement;
import rqlite.sql.SQLStatementDeserializer;
import rqlite.sql.SQLStatementSerializer;
import rqlite.sql.SQLStatements;
import rqlite.util.HttpClients;

/* The main rqlite client. Methods map closely to your Go client.
   For brevity, each method throws Exception on error. */
public class RqliteClient {

  private HttpClient httpClient;
  private String executeURL;
  private String queryURL;
  private String requestURL;
  private String statusURL;
  private String nodesURL;
  private String readyURL;

  private String basicAuthUser = "";
  private String basicAuthPass = "";
  private final ObjectMapper objectMapper;

  public RqliteClient(String baseURL, HttpClient client) {
    this.executeURL = baseURL + "/db/execute";
    this.queryURL = baseURL + "/db/query";
    this.requestURL = baseURL + "/db/request";
    this.statusURL = baseURL + "/status";
    this.nodesURL = baseURL + "/nodes";
    this.readyURL = baseURL + "/readyz";
    this.httpClient = (client != null) ? client : HttpClients.defaultHttpClient();

    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(SQLStatement.class, new SQLStatementSerializer());
    module.addDeserializer(SQLStatement.class, new SQLStatementDeserializer());
    objectMapper.registerModule(module);
  }

  public void setBasicAuth(String username, String password) {
    this.basicAuthUser = username;
    this.basicAuthPass = password;
  }

  public L4Response executeSingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return execute(stmts);
  }

  public L4Response execute(SQLStatements statements) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(executeURL + queryParams, body);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode() + ", body: " + new String(resp.body()));
    }
    var node = Json.parse(resp.body()).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public L4Response querySingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return query(stmts);
  }

  public L4Response query(SQLStatements statements) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(queryURL + queryParams, body);
    var node = Json.parse(resp.body()).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public L4Response requestSingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return request(stmts);
  }

  public L4Response request(SQLStatements statements) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(requestURL + queryParams, body);
    var node = Json.parse(resp.body()).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public JsonNode status() throws Exception {
    HttpResponse<byte[]> resp = doGetRequestBytes(statusURL);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return objectMapper.readTree(resp.body());
  }

  public JsonNode nodes() throws Exception {
    HttpResponse<byte[]> resp = doGetRequestBytes(nodesURL);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return objectMapper.readTree(resp.body());
  }

  public InputStream ready() throws Exception {
    HttpResponse<InputStream> resp = doGetRequest(readyURL);
    return resp.body();
  }

  private HttpResponse<String> doJSONPostRequest(String url, byte[] body) throws Exception {
    return doPostRequest(url, "application/json", body);
  }

  private HttpResponse<String> doPostRequest(String url, String contentType, byte[] body) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5));
    builder.method("POST", HttpRequest.BodyPublishers.ofByteArray(body));
    if (contentType != null && !contentType.isEmpty()) {
      builder.header("Content-Type", contentType);
    }
    addBasicAuth(builder);
    HttpRequest req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<InputStream> doGetRequest(String url) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET();
    addBasicAuth(builder);
    HttpRequest req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream());
  }

  // For endpoints where you need the full response as a byte array (for JSON parsing)
  private HttpResponse<byte[]> doGetRequestBytes(String url) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET();
    addBasicAuth(builder);
    HttpRequest req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
  }

  private void addBasicAuth(HttpRequest.Builder builder) {
    if (!basicAuthUser.isEmpty() || !basicAuthPass.isEmpty()) {
      String auth = basicAuthUser + ":" + basicAuthPass;
      String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
      builder.header("Authorization", "Basic " + encoded);
    }
  }

}

