package lbt.web

import cats.effect.{IO, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Sink, Stream}
import io.circe
import io.circe.generic.auto._
import io.circe.parser._
import lbt.WebsocketConfig
import lbt.models.{BusRoute, LatLngBounds}
import org.http4s.HttpService
import org.http4s.dsl.{->, :?, Http4sDsl, NotFound, Root, _}
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Text, WebSocketFrame}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

class WebSocketService(webSocketClientHandler: WebSocketClientHandler, websocketConfig: WebsocketConfig)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      val existsAlready = Await.result(webSocketClientHandler.isAlreadySubscribed(uuid), 10 seconds)
      if (existsAlready) InternalServerError(s"Not subscribed to WS feed, uuid $uuid already subscribed")
      else {
        webSocketClientHandler.subscribe(uuid)


        val toClient: Stream[IO, WebSocketFrame] =
          scheduler.awakeEvery[IO](websocketConfig.clientSendInterval).map { _ =>
            Text(Await.result(webSocketClientHandler.retrieveTransmissionDataForClient(uuid), 10 seconds)) //todo is this await the only option?
          }
        val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
          ws match {
            case Text(params, _) => F.delay(handleIncomingFilterParams(uuid, params))
            case f => F.delay(println(s"Unknown type: $f"))
          }
        }
        WS(toClient, fromClient)
      }
  }

  private def handleIncomingFilterParams(clientUUID: String, params: String): Unit = {
    println(s"Client $clientUUID received filter params $params")
    decodeIncomingFilterParams(params) match {
      case Right(filteringParams) =>
        logger.info(s"Successfully decoded filtering parameters for $clientUUID, filtering params: $filteringParams")
        webSocketClientHandler.updateFilteringParamsForClient(clientUUID, filteringParams)
      case Left(e) => logger.error(s"Error parsing/decoding filter params: $params", e)
    }
  }

  private def decodeIncomingFilterParams(params: String): Either[circe.Error, FilteringParams] = {
    parse(params).flatMap(json => json.as[FilteringParams])
  }
}
