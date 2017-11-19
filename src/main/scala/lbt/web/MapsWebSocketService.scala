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
import org.http4s.websocket.WebsocketBits.{Close, Text, WebSocketFrame}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import _root_.io.circe.parser._
import _root_.io.circe.generic.auto._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

class MapsWebSocketService(mapsClientHandler: MapsClientHandler, websocketConfig: WebsocketConfig)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

  object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      val existsAlready = Await.result(mapsClientHandler.isSubscribed(uuid), 10 seconds)
      if (existsAlready) InternalServerError(s"Not subscribed to WS feed, uuid $uuid already subscribed")
      else {
        mapsClientHandler.subscribe(uuid)

        val toClient: Stream[IO, WebSocketFrame] =
          scheduler.awakeEvery[IO](websocketConfig.clientSendInterval).map { _ =>
            IO.fromFuture(Eval.now(
              (for {
                isSubscribed <- mapsClientHandler.isSubscribed(uuid)
                if isSubscribed
                transmissionData <- mapsClientHandler.retrieveTransmissionDataForClient(uuid)
              } yield transmissionData).map(x => Text(x)))).unsafeRunSync()
          }

        val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
          ws match {
            case Close(_) => F.delay{
              logger.info(s"Received close request for uuid $uuid")
              mapsClientHandler.unsubscribe(uuid)
            }
            case Text(msg, _) => F.delay((for {
                json <- parse(msg)
                filteringParams <- json.as[FilteringParams]
              } yield filteringParams) match {
                  case Left(err) => logger.error("Unable to decode filter params message from websocket", err)
                  case Right(filteringParams) => mapsClientHandler.updateFilteringParams(uuid, filteringParams)
                })
            case f => F.delay(logger.error(s"Unknown message from client, type: $f"))
          }
        }
        WS(toClient, fromClient)
      }
  }
}
