package lbt.web

import java.util.concurrent.atomic.AtomicLong

import cats.effect.{IO, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Sink, Stream}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser._
import lbt.WebsocketConfig
import lbt.models.{BusRoute, LatLngBounds}
import org.http4s.HttpService
import org.http4s.dsl.{->, :?, Http4sDsl, Root, _}
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Text, WebSocketFrame}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


case class FilteringParams(busRoutes: List[BusRoute], latLngBounds: LatLngBounds)

object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

class WebSocketService(webSocketClientHandler: WebSocketClientHandler, websocketConfig: WebsocketConfig)(implicit F: Effect[IO]) extends Http4sDsl[IO] with StrictLogging {

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
            case Text(msg, _) => F.delay(handleIncomingMessage(uuid, msg))
            case f => F.delay(logger.error(s"Unknown type: $f"))
          }
        }
        WS(toClient, fromClient)
      }
  }

  private def handleIncomingMessage(clientUUID: String, message: String) = {
    val parsedMsg = parse(message)
    val msgType = parsedMsg.flatMap(msg => msg.hcursor.downField("type").as[String])
    (parsedMsg, msgType) match {
      case (Right(json), Right("UPDATE")) => handleUpdateFilterParams(clientUUID, json)
      case (Right(json), Right("REFRESH")) => handleRefreshRequest(clientUUID, json)
      case (Right(_), e) => e.fold(
        failure => logger.error(s"Unable to parse message type field", failure),
        str => logger.error(s"Unable to identify message type $str"))
      case (Left(e), _) => logger.error(s"Unable to parse json incoming message $message, ${e.message}", e.underlying)
    }
  }

  private def handleUpdateFilterParams(clientUUID: String, jsonMsg: Json): Future[Unit] = {
    logger.debug(s"Client $clientUUID received filter params $jsonMsg")
    jsonMsg.as[FilteringParams] match {
      case Right(filteringParams) =>
        webSocketClientHandler.updateFilteringParamsForClient(clientUUID, filteringParams)
      case Left(e) => Future(logger.error(s"Error parsing/decoding filter param for update request: $jsonMsg", e))
    }
  }

  private def handleRefreshRequest(clientUUID: String, jsonMsg: Json) = {
    jsonMsg.as[FilteringParams] match {
      case Right(filteringParams) => for {
        _ <- webSocketClientHandler.addInProgressDataToClientCache(clientUUID, filteringParams)
        _ = Thread.sleep(websocketConfig.clientSendInterval.toMillis - (System.currentTimeMillis() - timeLastTransmitted.get()))
        _ <- webSocketClientHandler.updateFilteringParamsForClient(clientUUID, filteringParams)
      } yield ()
      case Left(e) => Future(logger.error(s"Error parsing/decoding filter param for refresh requests: $jsonMsg", e))
    }
  }
}
