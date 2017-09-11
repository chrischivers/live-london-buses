package lbt.streaming

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink}
import akka.util.ByteString
import lbt.DataSourceConfig
import play.api.libs.ws.WSAuthScheme.DIGEST
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContextExecutor, Future}

class StreamingClient(config: DataSourceConfig, action: (String => Unit))(implicit actorSystem: ActorSystem) {
  implicit private val materializer: ActorMaterializer = ActorMaterializer()
  implicit private val ec: ExecutionContextExecutor = actorSystem.dispatcher

  private val wsClient = StandaloneAhcWSClient()
  private val futureResponse = wsClient.url(config.sourceUrl).withAuth(config.username, config.password, DIGEST).stream()

  def start(): Future[Long] = {
    futureResponse.flatMap {
      res =>
        res.bodyAsSource
          .via(Framing.delimiter(
            ByteString("\n"),
            maximumFrameLength = 256,
            allowTruncation = true))
          .map(_.utf8String)
          .drop(1)
          .runWith(Sink.fold[Long, String](0L) { (total, line) =>
            action(line)
            total + 1
          })
    }
  }

  def closeClient = wsClient.close()

}

//object StreamingClient extends App {
//  val client = new StreamingClient(ConfigLoader.defaultConfig.dataSourceConfig)
//  println(client.dataStream.next())
//
//}

//class StreamingClient(config: DataSourceConfig) extends StrictLogging {
//
//  private lazy val httpRequestConfig = buildHttpRequestConfig(config.timeout)
//  private lazy val httpAuthScope = buildAuthScope(config.authScopeURL, config.authScopePort)
//  private lazy val httpCredentialsProvider = buildHttpCredentialsProvider(config.username, config.password, httpAuthScope)
//  private lazy val httpClient: CloseableHttpClient = buildHttpClient(httpRequestConfig, httpCredentialsProvider)
//  private lazy val httpResponse: CloseableHttpResponse = getHttpResponse(httpClient, config.sourceUrl)
//  lazy val dataStream = getStream(httpResponse).iterator
//
//  private def getStream(httpResponse: CloseableHttpResponse): Stream[String] = {
//    httpResponse.getStatusLine.getStatusCode match {
//      case HttpStatus.SC_OK =>
//        val br = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
//        Stream.continually(br.readLine()).takeWhile(_ != null).drop(config.linesToDisregard)
//      case otherStatus: Int =>
//        logger.error(s"Error getting Stream Iterator. Http Status Code: $otherStatus")
//        throw new IllegalStateException("Unable to retrieve input stream")
//    }
//  }
//
//  private def getHttpResponse(client: CloseableHttpClient, url: String): CloseableHttpResponse = {
//    logger.info(s"getting http response for $url")
//    val httpGet = new HttpGet(url)
//    logger.debug(s"Http Get : ${httpGet.toString}")
//    client.execute(httpGet)
//  }
//
//  private def buildHttpClient(requestConfig: RequestConfig, credentialsProvider: CredentialsProvider): CloseableHttpClient = {
//    val client = HttpClientBuilder.create()
//    client.setDefaultRequestConfig(requestConfig)
//    client.setDefaultCredentialsProvider(credentialsProvider)
//    client.build()
//  }
//
//  private def buildHttpRequestConfig(connectionTimeout: Int): RequestConfig = {
//    val requestBuilder = RequestConfig.custom()
//    requestBuilder.setConnectionRequestTimeout(connectionTimeout)
//    requestBuilder.setConnectTimeout(connectionTimeout)
//    requestBuilder.build()
//  }
//
//  private def buildHttpCredentialsProvider(userName: String, password: String, authScope: AuthScope): BasicCredentialsProvider = {
//    val credentialsProvider = new BasicCredentialsProvider()
//    val credentials = new UsernamePasswordCredentials(userName, password)
//    credentialsProvider.setCredentials(authScope, credentials)
//    credentialsProvider
//  }
//
//  private def buildAuthScope(authScopeUrl: String, authScopePort: Int): AuthScope = {
//    new AuthScope(authScopeUrl, authScopePort)
//  }
//
//  def closeDataSource = {
//    logger.info("closing http client")
//    httpClient.close()
//    httpResponse.close()
//  }
//}

