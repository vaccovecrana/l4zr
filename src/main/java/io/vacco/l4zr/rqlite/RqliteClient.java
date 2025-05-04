package io.vacco.l4zr.rqlite;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import io.vacco.l4zr.jdbc.*;
import io.vacco.l4zr.json.*;

public class RqliteClient {

  private HttpClient httpClient;
  private String executeURL;
  private String queryURL;
  private String statusURL;
  private String nodesURL;
  private String readyURL;

  private String basicAuthUser = "";
  private String basicAuthPass = "";

  public RqliteClient(String baseURL, HttpClient client) {
    this.executeURL = baseURL + "/db/execute";
    this.queryURL = baseURL + "/db/query";
    this.statusURL = baseURL + "/status";
    this.nodesURL = baseURL + "/nodes";
    this.readyURL = baseURL + "/readyz";
    this.httpClient = (client != null) ? client : HttpClients.defaultHttpClient();
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
    var node = Json.parse(resp.body()).asObject();
    return new L4Response(resp.statusCode(), node);
  }

  public L4Response querySingle(String statement, Object... args) throws Exception {
    return query(new L4Statement().sql(statement).withPositionalParams(args));
  }

  // TODO error handling: "400 - invalid request"
  public L4Response query(L4Statement ... statements) throws Exception {
    var body = L4Statement.toArray(statements).toString();
    var queryParams = L4Options.queryParams();
    var resp = doJSONPostRequest(queryURL + queryParams, body);
    var node = Json.parse(resp.body()).asObject();
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
    var builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5));
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

}

