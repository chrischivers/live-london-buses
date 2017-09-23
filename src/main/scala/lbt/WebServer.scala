package lbt

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.{PostgresDB, RedisClient, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.web.LbtServlet
import org.http4s.server.ServerApp
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Properties.envOrNone

object WebServer extends StreamApp[IO] with StrictLogging {

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip: String = "0.0.0.0"
//  val pool: ExecutorService = Executors.newCachedThreadPool()
  implicit val actorSystem = ActorSystem()

  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)
  val definitions = new Definitions(routeDefinitionsTable)

  val redisClient = new RedisClient(config.redisDBConfig)

  val lbtServlet = new LbtServlet(redisClient, definitions)

  override def stream(args: List[String], requestShutdown: IO[Unit]) = {
    logger.info(s"Starting up servlet using port $port bound to ip $ip")
    BlazeBuilder[IO]
      .bindHttp(port, ip)
      .withIdleTimeout(3.minutes)
      .mountService(lbtServlet.service)
      .serve
  }
}