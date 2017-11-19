package lbt.web

import cats.Eval
import cats.effect.{IO, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Sink, Stream}
import lbt.WebsocketConfig
import lbt.models.{BusRoute, LatLngBounds}
import org.http4s.HttpService
import org.http4s.dsl._
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Text, WebSocketFrame}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import _root_.io.circe.parser._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

class MapsWebSocketService(mapsClientHandler: MapsClientHandler, websocketConfig: WebsocketConfig)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

  object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      val existsAlready = Await.result(mapsClientHandler.isAlreadySubscribed(uuid), 10 seconds)
      if (existsAlready) InternalServerError(s"Not subscribed to WS feed, uuid $uuid already subscribed")
      else {
        mapsClientHandler.subscribe(uuid)

        val toClient: Stream[IO, WebSocketFrame] =
          scheduler.awakeEvery[IO](websocketConfig.clientSendInterval).map { _ =>
            IO.fromFuture(Eval.now(mapsClientHandler.retrieveTransmissionDataForClient(uuid)
              .map(x => Text(x)))).unsafeRunSync()
          }

        val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
          ws match {
            case Text(msg, _) => F.delay(for {
                json <- parse(msg)
                filteringParams <- json.as[FilteringParams]
                _ <- mapsClientHandler.updateFilteringParams(uuid, filteringParams)
              } yield filteringParams)
            case f => F.delay(logger.error(s"Unknown message from client, type: $f"))
          }
        }
        WS(toClient, fromClient)
      }
  }
}
