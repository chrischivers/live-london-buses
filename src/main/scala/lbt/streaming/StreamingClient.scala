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