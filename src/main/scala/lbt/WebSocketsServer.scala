package lbt

import akka.actor.ActorSystem
import cats.effect.IO
import fs2._
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import lbt.web.{WebSocketClientHandler, WebSocketService}
import org.http4s.dsl.{Http4sDsl, _}
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp
import scala.concurrent.ExecutionContext.Implicits.global

object WebSocketsServer extends StreamApp[IO] with Http4sDsl[IO] {

  implicit val actorSystem = ActorSystem()
  val config = ConfigLoader.defaultConfig
  val redisSubscriberCache = new RedisSubscriberCache(config.redisDBConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisDBConfig, redisSubscriberCache)
  val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)
  val webSocketService = new WebSocketService(webSocketClientHandler, config.websocketConfig)

  def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, Nothing] =
    Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
      BlazeBuilder[IO]
        .bindHttp(config.websocketConfig.websocketPort)
        .withWebSockets(true)
        .mountService(webSocketService.service(scheduler), "/ws")
        .serve
    }
}