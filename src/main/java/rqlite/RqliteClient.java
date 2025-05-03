package rqlite;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import rqlite.schema.commands.ExecuteResponse;
import rqlite.schema.commands.QueryResponse;
import rqlite.schema.commands.RequestResponse;
import rqlite.schema.options.*;
import rqlite.sql.SQLStatement;
import rqlite.sql.SQLStatementDeserializer;
import rqlite.sql.SQLStatementSerializer;
import rqlite.sql.SQLStatements;
import rqlite.util.HttpClients;
import rqlite.util.URLUtils;

/* The main rqlite client. Methods map closely to your Go client.
   For brevity, each method throws Exception on error. */
public class RqliteClient {

  private HttpClient httpClient;
  private String executeURL;
  private String queryURL;
  private String requestURL;
  private String backupURL;
  private String loadURL;
  private String bootURL;
  private String statusURL;
  private String expvarURL;
  private String nodesURL;
  private String readyURL;

  private String basicAuthUser = "";
  private String basicAuthPass = "";
  private AtomicBoolean promoteErrors = new AtomicBoolean(false);
  private final ObjectMapper objectMapper;

  public RqliteClient(String baseURL, HttpClient client) {
    this.executeURL = baseURL + "/db/execute";
    this.queryURL = baseURL + "/db/query";
    this.requestURL = baseURL + "/db/request";
    this.backupURL = baseURL + "/db/backup";
    this.loadURL = baseURL + "/db/load";
    this.bootURL = baseURL + "/boot";
    this.statusURL = baseURL + "/status";
    this.expvarURL = baseURL + "/debug/vars";
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

  public void promoteErrors(boolean b) {
    this.promoteErrors.set(b);
  }

  public ExecuteResponse executeSingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return execute(stmts, null);
  }

  public ExecuteResponse execute(SQLStatements statements, ExecuteOptions opts) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = (opts != null) ? URLUtils.makeQueryString(opts) : "";
    HttpResponse<byte[]> resp = doJSONPostRequest(executeURL + queryParams, body);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode() +
        ", body: " + new String(resp.body()));
    }
    ExecuteResponse er = objectMapper.readValue(resp.body(), ExecuteResponse.class);
    if (promoteErrors.get() && er.hasError()) {
      throw new Exception("Statement error encountered");
    }
    return er;
  }

  public QueryResponse querySingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return query(stmts, null);
  }

  public QueryResponse query(SQLStatements statements, QueryOptions opts) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = (opts != null) ? URLUtils.makeQueryString(opts) : "";
    HttpResponse<byte[]> resp = doJSONPostRequest(queryURL + queryParams, body);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode() +
        ", body: " + new String(resp.body()));
    }
    QueryResponse qr = objectMapper.readValue(resp.body(), QueryResponse.class);
    if (promoteErrors.get() && qr.hasError()) {
      throw new Exception("Query error encountered");
    }
    return qr;
  }

  public RequestResponse requestSingle(String statement, Object... args) throws Exception {
    SQLStatement stmt = SQLStatement.newSQLStatement(statement, args);
    SQLStatements stmts = new SQLStatements();
    stmts.add(stmt);
    return request(stmts, null);
  }

  public RequestResponse request(SQLStatements statements, RequestOptions opts) throws Exception {
    byte[] body = objectMapper.writeValueAsBytes(statements);
    String queryParams = (opts != null) ? URLUtils.makeQueryString(opts) : "";
    HttpResponse<byte[]> resp = doJSONPostRequest(requestURL + queryParams, body);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode() +
        ", body: " + new String(resp.body()));
    }
    RequestResponse rr = objectMapper.readValue(resp.body(), RequestResponse.class);
    if (promoteErrors.get() && rr.hasError()) {
      throw new Exception("Request error encountered");
    }
    return rr;
  }

  public InputStream backup(BackupOptions opts) throws Exception {
    String queryParams = (opts != null) ? URLUtils.makeQueryString(opts) : "";
    HttpResponse<InputStream> resp = doGetRequestStream(backupURL + queryParams);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return resp.body();
  }

  public void load(InputStream in, LoadOptions opts) throws Exception {
    String queryParams = (opts != null) ? URLUtils.makeQueryString(opts) : "";
    byte[] first13 = new byte[13];
    if (in.read(first13) != 13) {
      throw new IOException("Unable to read first 13 bytes");
    }
    InputStream combined = new SequenceInputStream(new ByteArrayInputStream(first13), in);
    if (validSQLiteData(first13)) {
      doOctetStreamPostRequest(loadURL + queryParams, combined);
    } else {
      doPlainPostRequest(loadURL + queryParams, combined);
    }
  }

  public void boot(InputStream in) throws Exception {
    doOctetStreamPostRequest(bootURL, in);
  }

  public JsonNode status() throws Exception {
    HttpResponse<byte[]> resp = doGetRequestBytes(statusURL);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return objectMapper.readTree(resp.body());
  }

  public JsonNode expvar() throws Exception {
    HttpResponse<byte[]> resp = doGetRequestBytes(expvarURL);
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

  public void close() {
    // Nothing to close (HttpClient is designed to be reused)
  }

  private HttpResponse<byte[]> doJSONPostRequest(String url, byte[] body) throws Exception {
    return doRequest("POST", url, "application/json", body);
  }

  private HttpResponse<byte[]> doOctetStreamPostRequest(String url, InputStream bodyStream) throws Exception {
    byte[] body = bodyStream.readAllBytes();
    return doRequest("POST", url, "application/octet-stream", body);
  }

  private HttpResponse<byte[]> doPlainPostRequest(String url, InputStream bodyStream) throws Exception {
    byte[] body = bodyStream.readAllBytes();
    return doRequest("POST", url, "text/plain", body);
  }

  private HttpResponse<byte[]> doRequest(String method, String url, String contentType, byte[] body) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5));
    if ("GET".equalsIgnoreCase(method)) {
      builder.GET();
    } else {
      builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body));
    }
    if (contentType != null && !contentType.isEmpty()) {
      builder.header("Content-Type", contentType);
    }
    addBasicAuth(builder);
    HttpRequest req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
  }

  private HttpResponse<InputStream> doGetRequest(String url) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET();
    addBasicAuth(builder);
    HttpRequest req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream());
  }

  // For endpoints where you want an InputStream (e.g., backup, ready)
  private HttpResponse<InputStream> doGetRequestStream(String url) throws Exception {
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

  private boolean validSQLiteData(byte[] b) {
    if (b.length < 13) return false;
    String header = new String(b, 0, 13);
    return header.equals("SQLite format");
  }
}

