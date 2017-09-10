package lbt.streaming

import java.io.{BufferedReader, InputStreamReader}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink}
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import lbt.{ConfigLoader, DataSourceConfig}
import org.apache.http.HttpStatus
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}
import play.api.libs.ws.WSAuthScheme.DIGEST
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.Future

object PlayStreamingClient extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val config = ConfigLoader.defaultConfig.dataSourceConfig
  val wsClient = StandaloneAhcWSClient()
  val futureResponse = wsClient.url(config.sourceUrl).withAuth(config.username, config.password, DIGEST).stream()

  val bytesReturned: Future[Long] = futureResponse.flatMap {
    res =>
      // Count the number of bytes returned
      res.bodyAsSource
        .via(Framing.delimiter(
          ByteString("\n"),
          maximumFrameLength = 256,
          allowTruncation = false))
        .map(_.utf8String)
        .runWith(Sink.fold[Long, String](0L) { (total, line) =>
          println(line)
          total + 1
        })
  }
}

object StreamingClient extends App {
  val client = new StreamingClient(ConfigLoader.defaultConfig.dataSourceConfig)
  println(client.dataStream.next())

}

class StreamingClient(config: DataSourceConfig) extends StrictLogging {

  private lazy val httpRequestConfig = buildHttpRequestConfig(config.timeout)
  private lazy val httpAuthScope = buildAuthScope(config.authScopeURL, config.authScopePort)
  private lazy val httpCredentialsProvider = buildHttpCredentialsProvider(config.username, config.password, httpAuthScope)
  private lazy val httpClient: CloseableHttpClient = buildHttpClient(httpRequestConfig, httpCredentialsProvider)
  private lazy val httpResponse: CloseableHttpResponse = getHttpResponse(httpClient, config.sourceUrl)
  lazy val dataStream = getStream(httpResponse).iterator

  private def getStream(httpResponse: CloseableHttpResponse): Stream[String] = {
    httpResponse.getStatusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        val br = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
        Stream.continually(br.readLine()).takeWhile(_ != null).drop(config.linesToDisregard)
      case otherStatus: Int =>
        logger.error(s"Error getting Stream Iterator. Http Status Code: $otherStatus")
        throw new IllegalStateException("Unable to retrieve input stream")
    }
  }

  private def getHttpResponse(client: CloseableHttpClient, url: String): CloseableHttpResponse = {
    logger.info(s"getting http response for $url")
    val httpGet = new HttpGet(url)
    logger.debug(s"Http Get : ${httpGet.toString}")
    client.execute(httpGet)
  }

  private def buildHttpClient(requestConfig: RequestConfig, credentialsProvider: CredentialsProvider): CloseableHttpClient = {
    val client = HttpClientBuilder.create()
    client.setDefaultRequestConfig(requestConfig)
    client.setDefaultCredentialsProvider(credentialsProvider)
    client.build()
  }

  private def buildHttpRequestConfig(connectionTimeout: Int): RequestConfig = {
    val requestBuilder = RequestConfig.custom()
    requestBuilder.setConnectionRequestTimeout(connectionTimeout)
    requestBuilder.setConnectTimeout(connectionTimeout)
    requestBuilder.build()
  }

  private def buildHttpCredentialsProvider(userName: String, password: String, authScope: AuthScope): BasicCredentialsProvider = {
    val credentialsProvider = new BasicCredentialsProvider()
    val credentials = new UsernamePasswordCredentials(userName, password)
    credentialsProvider.setCredentials(authScope, credentials)
    credentialsProvider
  }

  private def buildAuthScope(authScopeUrl: String, authScopePort: Int): AuthScope = {
    new AuthScope(authScopeUrl, authScopePort)
  }

  def closeDataSource = {
    logger.info("closing http client")
    httpClient.close()
    httpResponse.close()
  }
}

