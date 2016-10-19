package teleporter.integration.component.tcp

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.NotUsed
import akka.http.scaladsl.coding.Gzip
import akka.stream.TLSProtocol._
import akka.stream._
import akka.stream.scaladsl.Tcp.IncomingConnection
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.ExecutionContext

/**
 * date 2015/8/3.
 * @author daikui
 */
class TlsHelper

object TlsHelper {
  def defaultCipherSuites = NegotiateNewSession
    .withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA")
    .withClientAuth(TLSClientAuth.need)

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

  def clientTls(closing: TLSClosing)(implicit sslContext: SSLContext, cipherSuites: NegotiateNewSession): scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] = TLS(sslContext, None, cipherSuites, Client, closing)

  def serverTls(closing: TLSClosing)(implicit sslContext: SSLContext, cipherSuites: NegotiateNewSession): scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] = TLS(sslContext, None, cipherSuites, Server, closing)

  def sslTlsWrapper: BidiFlow[ByteString, SslTlsOutbound, SslTlsInbound, ByteString, NotUsed] =
    BidiFlow.fromFlows(
      Flow[ByteString].map(SendBytes),
      Flow[SslTlsInbound].map {
        case SessionBytes(a, b) ⇒ b
        case SessionTruncated ⇒ ByteString.empty
      })

  def serverVia(connection: IncomingConnection, maximumMessageLength: Int)
               (implicit ec: ExecutionContext, sslContext: SSLContext, newSession: NegotiateNewSession): Flow[ByteString, ByteString, NotUsed] = {
    Framing.simpleFramingProtocol(10 * 1024 * 1024).atop(TlsHelper.sslTlsWrapper).join(serverTls(TLSClosing.eagerClose).join(connection.flow))
  }

  def clientVia(tlsOutgoingFlow: Flow[SslTlsOutbound, SslTlsInbound, NotUsed], maximumMessageLength: Int)
               (implicit ec: ExecutionContext, sslContext: SSLContext, newSession: NegotiateNewSession): Flow[ByteString, ByteString, NotUsed] =
    Framing.simpleFramingProtocol(10 * 1024 * 1024).atop(TlsHelper.sslTlsWrapper).join(tlsOutgoingFlow)

  def gzipServerVia(connection: IncomingConnection, maximumMessageLength: Int, decodeParallelism: Int = 1)
                   (implicit mater: Materializer, ec: ExecutionContext, sslContext: SSLContext, newSession: NegotiateNewSession): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].map(Gzip.encode).via(serverVia(connection, maximumMessageLength)).mapAsyncUnordered(decodeParallelism)(Gzip.decode)

  def gzipClientVia(tlsOutgoingFlow: Flow[SslTlsOutbound, SslTlsInbound, NotUsed], maximumMessageLength: Int, decodeParallelism: Int = 1)
                   (implicit mater: Materializer, ec: ExecutionContext, sslContext: SSLContext, newSession: NegotiateNewSession): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].map(Gzip.encode).via(clientVia(tlsOutgoingFlow, maximumMessageLength)).mapAsyncUnordered(decodeParallelism)(Gzip.decode)
}
