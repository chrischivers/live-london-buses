package lbt

import akka.actor.ActorSystem
import cats.effect.IO
import fs2._
import lbt.db.caching.{RedisArrivalTimeLog, RedisSubscriberCache, RedisWsClientCache}
import lbt.web.{MapsClientHandler, MapsWebSocketService}
import org.http4s.dsl.{Http4sDsl, _}
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.{ExitCode, StreamApp}

import scala.concurrent.ExecutionContext.Implicits.global

object WebSocketsServer extends StreamApp[IO] with Http4sDsl[IO] {

  implicit val actorSystem = ActorSystem()
  val config = ConfigLoader.defaultConfig
  val redisSubscriberCache = new RedisSubscriberCache(config.redisConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisConfig, redisSubscriberCache)
  val redisArrivalTimeLog = new RedisArrivalTimeLog(config.redisConfig)
  val webSocketClientHandler = new MapsClientHandler(redisSubscriberCache, redisWsClientCache, redisArrivalTimeLog)
  val webSocketService = new MapsWebSocketService(webSocketClientHandler, config.websocketConfig)

  def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, ExitCode] =
    Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
      BlazeBuilder[IO]
        .bindHttp(config.websocketConfig.websocketPort)
        .withWebSockets(true)
        .mountService(webSocketService.service(scheduler), "/ws")
        .serve
    }
}