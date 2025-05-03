package rqlite.util;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.net.http.HttpClient;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;

/* A set of helper methods to create HttpClient instances with various TLS settings. */
public class HttpClients {

  public static HttpClient defaultHttpClient() {
    return HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();
  }

  public static HttpClient newTLSSClientInsecure() throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    TrustManager[] trustAll = new TrustManager[]{
      new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
    sslContext.init(null, trustAll, new SecureRandom());
    return HttpClient.newBuilder()
      .sslContext(sslContext)
      .connectTimeout(Duration.ofSeconds(5))
      .build();
  }

  public static HttpClient newTLSSClient(String caCertPath) throws Exception {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    byte[] caBytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(caCertPath));
    ByteArrayInputStream bis = new ByteArrayInputStream(caBytes);
    X509Certificate caCert = (X509Certificate) cf.generateCertificate(bis);

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("caCert", caCert);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
    return HttpClient.newBuilder()
      .sslContext(sslContext)
      .connectTimeout(Duration.ofSeconds(5))
      .build();
  }

  public static HttpClient newMutualTLSClient(String clientCertPath, String clientKeyPath, String caCertPath) throws Exception {
    // Loading client cert and key from separate PEM files is not straightforward in Java.
    // Typically one uses a PKCS12 keystore. For now, we throw an exception.
    throw new UnsupportedOperationException("Mutual TLS from separate PEM files is not implemented. Use a PKCS12 keystore instead.");
  }
}
