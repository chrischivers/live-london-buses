package lbt.web

import cats.effect.{IO, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Sink, Stream}
import io.circe
import lbt.models.{BusRoute, LatLngBounds}
import org.http4s.HttpService
import org.http4s.dsl.{->, :?, Http4sDsl, Root, _}
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Text, WebSocketFrame}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

class WebSocketService(webSocketClientHandler: WebSocketClientHandler)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      webSocketClientHandler.subscribe(uuid)

      val toClient: Stream[IO, WebSocketFrame] =
        scheduler.awakeEvery[IO](1.seconds).map { _ =>
          Text(Await.result(webSocketClientHandler.getDataForClient(uuid), 10 seconds)) //todo is this await the only option?
        }
      val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
        ws match {
          case Text(params, _) => F.delay(handleIncomingFilterParams(uuid, params))
          case f => F.delay(println(s"Unknown type: $f"))
        }
      }
      WS(toClient, fromClient)
  }

  private def handleIncomingFilterParams(clientUUID: String, params: String): Unit = {
    decodeIncomingFilterParams(params) match {
      case Right(filteringParams) => webSocketClientHandler.updateFilteringParamsForClient(clientUUID, filteringParams)
      case Left(e) => logger.error(s"Error parsing/decoding filter params: $params", e)
    }
  }

  private def decodeIncomingFilterParams(params: String): Either[circe.Error, FilteringParams] = {
    parse(params).flatMap(json => json.as[FilteringParams])
  }
}
