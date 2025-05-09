package io.vacco.l4zr.rqlite;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import io.vacco.l4zr.json.*;

import static java.lang.String.format;

public class L4Client {

  private final HttpClient httpClient;
  private final String executeURL;
  private final String queryURL;
  private final String statusURL;
  private final String nodesURL;
  private final String readyURL;

  private String basicAuthUser = "";
  private String basicAuthPass = "";

  public L4Client(String baseURL, HttpClient client) {
    this.executeURL = baseURL + "/db/execute";
    this.queryURL = baseURL + "/db/query";
    this.statusURL = baseURL + "/status";
    this.nodesURL = baseURL + "/nodes";
    this.readyURL = baseURL + "/readyz";
    this.httpClient = (client != null) ? client : L4Http.defaultHttpClient();
  }

  public void setBasicAuth(String username, String password) {
    this.basicAuthUser = username;
    this.basicAuthPass = password;
  }

  public L4Response executeSingle(String statement, Object... args) throws Exception {
    return execute(new L4Statement().sql(statement).withPositionalParams(args));
  }

  public L4Response execute(L4Statement ... statements) throws Exception {
    var body = L4Statement.toArray(statements).toString();
    var queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(executeURL + queryParams, body);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode() + ", body: " + resp.body());
    }
    var rb = resp.body();
    var node = Json.parse(rb).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public L4Response querySingle(String statement, Object... args) throws Exception {
    return query(new L4Statement().sql(statement).withPositionalParams(args));
  }

  public L4Response query(L4Statement ... statements) throws Exception {
    var body = L4Statement.toArray(statements).toString();
    var queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(queryURL + queryParams, body);
    var rb = resp.body();
    var node = Json.parse(rb).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public JsonValue status() throws Exception {
    var resp = doGetRequest(statusURL);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return Json.parse(resp.body());
  }

  public JsonValue nodes() throws Exception {
    var resp = doGetRequest(nodesURL);
    if (resp.statusCode() != 200) {
      throw new IOException("Unexpected status code: " + resp.statusCode());
    }
    return Json.parse(resp.body());
  }

  public String ready() throws Exception {
    var resp = doGetRequest(readyURL);
    return resp.body();
  }

  private HttpResponse<String> doJSONPostRequest(String url, String body) throws Exception {
    return doPostRequest(url, "application/json", body);
  }

  private HttpResponse<String> doPostRequest(String url, String contentType, String body) throws Exception {
    var builder = HttpRequest.newBuilder().uri(URI.create(url));
    if (L4Options.timeoutSec > 0) {
      builder.timeout(Duration.ofSeconds(L4Options.timeoutSec));
    }
    builder.method("POST", HttpRequest.BodyPublishers.ofString(body));
    if (contentType != null && !contentType.isEmpty()) {
      builder.header("Content-Type", contentType);
    }
    addBasicAuth(builder);
    var req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> doGetRequest(String url) throws Exception {
    var builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET();
    addBasicAuth(builder);
    if (L4Options.timeoutSec > 0) {
      builder.timeout(Duration.ofSeconds(L4Options.timeoutSec));
    }
    var req = builder.build();
    return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
  }

  private void addBasicAuth(HttpRequest.Builder builder) {
    if (!basicAuthUser.isEmpty() || !basicAuthPass.isEmpty()) {
      String auth = basicAuthUser + ":" + basicAuthPass;
      String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
      builder.header("Authorization", "Basic " + encoded);
    }
  }

  public L4Client withTxTimeoutSec(long txTimeoutSec) {
    if (txTimeoutSec < 0) {
      throw new IllegalArgumentException(format("Invalid timeout [%d]", txTimeoutSec));
    }
    L4Options.timeoutSec = txTimeoutSec == 0 ? -1 : txTimeoutSec;
    return this;
  }

  public long getTxTimeoutSec() {
    return L4Options.timeoutSec;
  }

}

