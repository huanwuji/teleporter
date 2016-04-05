package teleporter.integration.component

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.stream.io._

/**
 * date 2015/8/3.
 * @author daikui
 */
class TlsHelper

object TlsHelper {
  def defaultCipherSuites = NegotiateNewSession
    .withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA")
    .withClientAuth(ClientAuth.need)

  def initSslContext(password: Array[Char], keyStoreStream: InputStream, trustStoreStream: InputStream): SSLContext = {
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(keyStoreStream, password)
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(trustStoreStream, password)
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password)
    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)
    val context = SSLContext.getInstance("TLS")
    context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
    context
  }

  def clientTls(closing: Closing)(implicit sslContext: SSLContext, cipherSuites: NegotiateNewSession) = SslTls(sslContext, cipherSuites, Client, closing)

  def serverTls(closing: Closing)(implicit sslContext: SSLContext, cipherSuites: NegotiateNewSession) = SslTls(sslContext, cipherSuites, Server, closing)
}
