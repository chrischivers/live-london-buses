package lbt

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching.RedisDurationRecorder
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.streaming._

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

  val redisClient = new RedisDurationRecorder(config.redisDBConfig)

  val streamingClient = new StreamingClient(dataSourceClient, processSourceLine)

  val cache = ScalaCache(GuavaCache())
  val sourceLineHandler = new SourceLineHandler(definitions, config.sourceLineHandlerConfig, redisClient)(cache, ec)

  streamingClient.start().map(_ => ())

  def processSourceLine(rawSourceLine: String): Unit = {
    SourceLine.fromRawLine(rawSourceLine).fold(throw new RuntimeException(s"Unable to parse raw source line: $rawSourceLine")) { line =>
      if (line.validate(definitions)) sourceLineHandler.handle(line).value
    }
  }

}
