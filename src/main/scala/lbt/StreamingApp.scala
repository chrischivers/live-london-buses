package lbt

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching.{RedisArrivalTimeLog, RedisSubscriberCache, RedisVehicleArrivalTimeLog, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.metrics.MetricsLogging
import lbt.streaming._
import lbt.web.MapsClientHandler
import scala.concurrent.ExecutionContext

object StreamingApp extends App with StrictLogging {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  val config = ConfigLoader.defaultConfig

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema())
  val definitions = new Definitions(routeDefinitionsTable)

  val redisSubscriberCache = new RedisSubscriberCache(config.redisConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisConfig, redisSubscriberCache)
  val redisArrivalTimeLog = new RedisArrivalTimeLog(config.redisConfig)

  val webSocketClientHandler = new MapsClientHandler(redisSubscriberCache, redisWsClientCache, redisArrivalTimeLog)

  val redisVehicleArrivalTimeLog = new RedisVehicleArrivalTimeLog(config.redisConfig, config.streamingConfig)

  val sourceLineHandler = new SourceLineHandler(redisArrivalTimeLog, redisVehicleArrivalTimeLog, definitions, config.streamingConfig)

  val streamingClient = new StreamingClient(config.dataSourceConfig, processSourceLine)

  val cachedReaderScheduler = new CacheReaderScheduler(
    redisArrivalTimeLog,
    redisVehicleArrivalTimeLog,
    redisSubscriberCache,
    redisWsClientCache,
    definitions,
    config.streamingConfig)

  logger.info("Starting streaming client")
  streamingClient.start().map(_ => ())

  def processSourceLine(rawSourceLine: String): Unit = {
    MetricsLogging.incrSourceLinesReceived
    SourceLine.fromRawLine(rawSourceLine).fold(throw new RuntimeException(s"Unable to parse raw source line: $rawSourceLine")) { line =>
      if (line.validate(definitions)) {
        MetricsLogging.incrSourceLinesValidated
        sourceLineHandler.handle(line)
      }
    }
  }

}
