package lbt

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.web.MapService
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Properties.envOrNone

object HttpWebServer extends StreamApp[IO] with StrictLogging {

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip: String = "0.0.0.0"
  //  val pool: ExecutorService = Executors.newCachedThreadPool()
  implicit val actorSystem = ActorSystem()

  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)
  val definitions = new Definitions(routeDefinitionsTable)
  val redisSubscriberCache = new RedisSubscriberCache(config.redisConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisConfig,redisSubscriberCache)

  val mapService = new MapService(definitions, redisWsClientCache)


  override def stream(args: List[String], requestShutdown: IO[Unit]) = {
    logger.info(s"Starting up servlet using port $port bound to ip $ip")
    BlazeBuilder[IO]
      .bindHttp(port, ip)
      .withIdleTimeout(3.minutes)
      .withWebSockets(true)
      .mountService(mapService.service, "/map")
      .serve
  }
}