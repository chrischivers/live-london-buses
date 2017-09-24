package lbt.web

import akka.actor.ActorSystem
import cats.effect.{IO, _}
import fs2.{Scheduler, Sink, Stream}
import lbt.ConfigLoader
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import org.http4s.HttpService
import org.http4s.dsl.{->, :?, Http4sDsl, Root, _}
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Text, WebSocketFrame}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

class WebSocketService(webSocketClientHandler: WebSocketClientHandler)(implicit F: Effect[IO]) extends Http4sDsl[IO] {

  def service(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {

    case GET -> Root :? UUIDQueryParameter(uuid) =>

      webSocketClientHandler.subscribe(uuid)

      val toClient: Stream[IO, WebSocketFrame] =
        scheduler.awakeEvery[IO](1.seconds).map { _ =>
          Text(Await.result(webSocketClientHandler.getDataForClient(uuid), 10 seconds)) //todo is this await the only option?
        }
      val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
        ws match {
          case Text(t, _) => F.delay(println(t)) //todo change bounds/filtering parameters here
          case f => F.delay(println(s"Unknown type: $f"))
        }
      }
      WS(toClient, fromClient)
  }

}
