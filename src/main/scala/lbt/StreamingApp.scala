package lbt

import akka.actor.ActorSystem
import lbt.common.Definitions
import lbt.db.{PostgresDB, RedisClient, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.streaming.{SourceLine, SourceLineHandler, StreamingClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scalacache.ScalaCache
import scalacache.guava.GuavaCache

object StreamingApp extends App {
  implicit val actorSystem = ActorSystem()
  implicit val ec = ExecutionContext.Implicits.global
  val config = ConfigLoader.defaultConfig

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = true)
  val definitions = new Definitions(routeDefinitionsTable)

  val streamingClient = new StreamingClient(config.dataSourceConfig, processSourceLine)(actorSystem)
  val redisClient = new RedisClient(config.redisDBConfig)

  val cache = ScalaCache(GuavaCache())
  val sourceLineHandler = new SourceLineHandler(definitions, config.sourceLineHandlerConfig, redisClient)(cache, ec)

  Await.ready(streamingClient.start(), 100 days)


  def processSourceLine(rawSourceLine: String): Unit = {
    SourceLine.fromRawLine(rawSourceLine).fold(throw new RuntimeException(s"Unable to parse raw source line: $rawSourceLine")){ line =>
      if (line.validate(definitions)) sourceLineHandler.handle(line)
    }
  }

}
