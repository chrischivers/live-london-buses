package lbt

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.metrics.MetricsLogging
import lbt.streaming._
import lbt.web.WebSocketClientHandler

import scala.concurrent.ExecutionContext


object StreamingApp extends App with StrictLogging {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  val config = ConfigLoader.defaultConfig

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema())
  val definitions = new Definitions(routeDefinitionsTable)

  val redisSubscriberCache = new RedisSubscriberCache(config.redisDBConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisDBConfig, redisSubscriberCache)

  val streamingClient = new StreamingClient(config.dataSourceConfig, processSourceLine)
  val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)

  streamingClient.start().map(_ => ())

  def processSourceLine(rawSourceLine: String): Unit = {
    MetricsLogging.incrSourceLinesReceived
    SourceLine.fromRawLine(rawSourceLine).fold(throw new RuntimeException(s"Unable to parse raw source line: $rawSourceLine")) { line =>
//      if (line.vehicleID == "LTZ1703") logger.info("Line received: " + line)
      if (line.validate(definitions)) {
//        if (line.vehicleID == "LTZ1703") logger.info("Line validated: " + line)
        MetricsLogging.incrSourceLinesValidated
        //TODO something here
      }
    }
  }

}
