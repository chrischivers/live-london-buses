package lbt.web

import java.util.concurrent.atomic.AtomicLong

import cats.effect.{IO, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Sink, Stream}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser._
import lbt.WebsocketConfig
import lbt.metrics.MetricsLogging
import lbt.models.{BusRoute, LatLngBounds}
import org.http4s.HttpService
import org.http4s.dsl.{->, :?, Http4sDsl, Root, _}
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Close, Text, WebSocketFrame}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

class WebSocketService(webSocketClientHandler: WebSocketClientHandler, websocketConfig: WebsocketConfig)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

  object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

  val timeLastTransmitted = new AtomicLong(0L)

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      val existsAlready = Await.result(webSocketClientHandler.isAlreadySubscribed(uuid), 10 seconds)
      if (existsAlready) InternalServerError(s"Not subscribed to WS feed, uuid $uuid already subscribed")
      else {
        webSocketClientHandler.subscribe(uuid)

        val toClient: Stream[IO, WebSocketFrame] =
          scheduler.awakeEvery[IO](websocketConfig.clientSendInterval).map { _ =>
            timeLastTransmitted.set(System.currentTimeMillis())
            Text(Await.result(webSocketClientHandler.retrieveTransmissionDataForClient(uuid), 10 seconds)) //todo is this await the only option?
          }

        val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
          ws match {
//            case Text(msg, _) => F.delay(handleIncomingMessage(uuid, msg))
            case f => F.delay(logger.error(s"Unknown message from client, type: $f"))
          }
        }
        WS(toClient, fromClient)
      }
  }
}
