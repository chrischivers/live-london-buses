package lbt.streaming

import java.io.{BufferedReader, InputStreamReader}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import lbt.DataSourceConfig
import org.apache.http.HttpStatus
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}

import scala.concurrent.{ExecutionContextExecutor, Future}


class StreamingClient(busDataSourceClient: BusDataSourceClient, action: (String => Unit))(implicit actorSystem: ActorSystem) extends StrictLogging {

  implicit private val materializer: ActorMaterializer = ActorMaterializer()
  implicit private val ec: ExecutionContextExecutor = actorSystem.dispatcher

  lazy val dataStream: Iterator[String] = getStream(busDataSourceClient.getHttpResponse).iterator

  def start(): Future[Long] = {
    logger.info("Starting streaming client")
    Source.fromIterator(() => dataStream).runFold[Long](0L) { (total, line) =>
      if(total % 10000 == 0) logger.info(s"$total streamed rows processed")
      action(line)
      total + 1
    }
  }

  private def getStream(httpResponse: CloseableHttpResponse) = {
    httpResponse.getStatusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        val br = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
        Stream.continually(br.readLine()).takeWhile(_ != null).drop(1)
      case otherStatus: Int =>
        logger.error(s"Error getting Stream Iterator. Http Status Code: $otherStatus")
        throw new IllegalStateException("Unable to retrieve input stream")
    }
  }

  def close = busDataSourceClient.closeDataSource()
}


class BusDataSourceClient(config: DataSourceConfig) extends StrictLogging {

  private val httpRequestConfig = buildHttpRequestConfig(config.timeout)
  private val httpAuthScope = buildAuthScope(config.authScopeURL, config.authScopePort)
  private val httpCredentialsProvider = buildHttpCredentialsProvider(config.username, config.password, httpAuthScope)
  private val httpClient: CloseableHttpClient = buildHttpClient(httpRequestConfig, httpCredentialsProvider)
  private lazy val httpResponse: CloseableHttpResponse = executeHttpRequest

  def getHttpResponse: CloseableHttpResponse = httpResponse

  private def executeHttpRequest: CloseableHttpResponse = {
    val httpGet = new HttpGet(config.sourceUrl)
    httpClient.execute(httpGet)
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

  def closeDataSource() = {
    logger.info("closing http client")
    httpClient.close()
    httpResponse.close()
  }

}