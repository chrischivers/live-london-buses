package lbt

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, waitTimeAfterClose: Int, cacheTimeToLiveSeconds: Int, timeWindowToAcceptLines: Int, numberEmptyIteratorCasesBeforeRestart: Int, simulationIterator: Option[Iterator[String]] = None)

case class DefinitionsConfig(sourceAllUrl: String, sourceSingleUrl: String, definitionsCachedTime: Int)

sealed trait DBConfig {
  val host: String
  val port: Int
  val username: String
  val password: String
  val dbName: String
}

case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig

case class RedisConfig(host: String, port: Int, dbIndex: Int, maxListLength: Int)

case class SourceLineHandlerConfig(cacheTtl: Duration, minimumTimeDifferenceToPersist: Duration)

case class LBTConfig(
                      dataSourceConfig: DataSourceConfig,
                      postgresDbConfig: PostgresDBConfig,
                      redisDBConfig: RedisConfig,
                      definitionsConfig: DefinitionsConfig,
                      sourceLineHandlerConfig: SourceLineHandlerConfig)

object ConfigLoader {

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val dataBaseParamsPrefix = "database."
    val definitionsParamsPrefix = "dataSource.definitions."
    val postgresDBParamsPrefix = "db.postgres."
    val redisDBParamsPrefix = "db.redis."
    val sourceLineHandlerParamsPrefix = "source-line-handler."
    LBTConfig(
      DataSourceConfig(
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "source-url"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "username"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "password"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "authscope-url"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "authscope-port"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "connection-timeout"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "wait-time-after-close"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "cache-time-to-live-seconds"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "time-window-to-accept-lines"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "number-empty-iterator-cases-before-restart"),
//        defaultConfigFactory.getStringList(dataSourceStreamingParamsPrefix + "get-only-routes").toList.map(rec => parse(rec).extract[BusRoute]) match {
//          case Nil => None
//          case x => Some(x)
//        }
      ),
      PostgresDBConfig(
        defaultConfigFactory.getString(postgresDBParamsPrefix + "host"),
        defaultConfigFactory.getInt(postgresDBParamsPrefix + "port"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "username"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "password"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "dbName")
      ),
      RedisConfig(
        defaultConfigFactory.getString(redisDBParamsPrefix + "host"),
        defaultConfigFactory.getInt(redisDBParamsPrefix + "port"),
        defaultConfigFactory.getInt(redisDBParamsPrefix + "dbIndex"),
        defaultConfigFactory.getInt(redisDBParamsPrefix + "maxListLength"),
      ),
      DefinitionsConfig(
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-all-url"),
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-single-url"),
        defaultConfigFactory.getInt(definitionsParamsPrefix + "definitions-cached-time")
      ),
      SourceLineHandlerConfig(
        defaultConfigFactory.getDuration(sourceLineHandlerParamsPrefix + "cache-ttl"),
        defaultConfigFactory.getDuration(sourceLineHandlerParamsPrefix + "minimum-time-difference")
      )
    )

  }
}

