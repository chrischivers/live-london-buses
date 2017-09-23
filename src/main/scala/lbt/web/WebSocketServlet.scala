package lbt.web

import akka.actor.ActorSystem
import cats.effect.{IO, _}
import fs2._
import lbt.ConfigLoader
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import org.http4s._
import org.http4s.dsl.{Http4sDsl, Root, _}
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.server.websocket._
import org.http4s.util.StreamApp
import org.http4s.websocket.WebsocketBits._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

object WebSocketServletApp extends WebSocketServlet

class WebSocketServlet(implicit F: Effect[IO]) extends StreamApp[IO] with Http4sDsl[IO] {

  implicit val actorSystem = ActorSystem()
  val config = ConfigLoader.defaultConfig
  val redisSubscriberCache = new RedisSubscriberCache(config.redisDBConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisDBConfig)
  val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)

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

  def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, Nothing] =
    Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
      BlazeBuilder[IO]
        .bindHttp(8080)
        .withWebSockets(true)
        .mountService(service(scheduler), "/ws")
        .serve
    }

}