package lbt

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching.{RedisDurationRecorder, RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.metrics.MetricsLogging
import lbt.streaming._
import lbt.web.WebSocketClientHandler

import scala.concurrent.ExecutionContext
import scalacache.ScalaCache
import scalacache.guava.GuavaCache


object StreamingApp extends App with StrictLogging {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  val config = ConfigLoader.defaultConfig

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema())
  val definitions = new Definitions(routeDefinitionsTable)

  val dataSourceClient = new BusDataSourceClient(config.dataSourceConfig)
  val redisDurationRecorder = new RedisDurationRecorder(config.redisDBConfig)
  val redisSubscriberCache = new RedisSubscriberCache(config.redisDBConfig)
  val redisWsClientCache = new RedisWsClientCache(config.redisDBConfig, redisSubscriberCache)

  val streamingClient = new StreamingClient(dataSourceClient, processSourceLine)
  val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)

  val cache = ScalaCache(GuavaCache())
  val sourceLineHandler = new SourceLineHandler(definitions, config.sourceLineHandlerConfig, redisDurationRecorder, webSocketClientHandler)(cache, ec)

  streamingClient.start().map(_ => ())

  def processSourceLine(rawSourceLine: String): Unit = {
    MetricsLogging.incrSourceLinesReceived
    SourceLine.fromRawLine(rawSourceLine).fold(throw new RuntimeException(s"Unable to parse raw source line: $rawSourceLine")) { line =>
      if (line.validate(definitions)) {
        MetricsLogging.incrSourceLinesValidated
        sourceLineHandler.handle(line).value
      }
    }
  }

}
