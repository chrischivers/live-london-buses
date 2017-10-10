package lbt

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import lbt.ConfigLoader.defaultConfigFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, waitTimeBeforeRestart: Int, cacheTimeToLiveSeconds: Int, timeWindowToAcceptLines: Int)

case class DefinitionsConfig(sourceAllUrl: String, sourceSingleUrl: String, definitionsCachedTime: Int, directionsApiKeys: List[String])

sealed trait DBConfig {
  val host: String
  val port: Int
  val username: String
  val password: String
  val dbName: String
}

case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig

case class RedisConfig(host: String, port: Int, dbIndex: Int, wsClientCacheMaxResultsReturned: Int, clientInactiveTime: Duration)

case class WebsocketConfig(clientSendInterval: FiniteDuration, websocketPort: Int)

case class StreamingConfig(idleTimeBeforeVehicleDeleted: FiniteDuration, cleanUpEveryNLines: Int)

case class MetricsConfig(host: String, port: Int, dbName: String, updateInterval: Int, enabled: Boolean)

case class LBTConfig(
                      dataSourceConfig: DataSourceConfig,
                      postgresDbConfig: PostgresDBConfig,
                      redisDBConfig: RedisConfig,
                      definitionsConfig: DefinitionsConfig,
                      websocketConfig: WebsocketConfig,
                      streamingConfig: StreamingConfig,
                      metricsConfig: MetricsConfig)

object ConfigLoader {

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val definitionsParamsPrefix = "dataSource.definitions."
    val postgresDBParamsPrefix = "db.postgres."
    val redisDBParamsPrefix = "db.redis."

    LBTConfig(
      DataSourceConfig(
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "source-url"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "username"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "password"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "authscope-url"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "authscope-port"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "connection-timeout"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "wait-time-before-restart"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "cache-time-to-live-seconds"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "time-window-to-accept-lines")
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
        defaultConfigFactory.getInt(redisDBParamsPrefix + "wsClientCacheMaxResultsReturned"),
        defaultConfigFactory.getDuration(redisDBParamsPrefix + "clientInactiveTime")
      ),
      DefinitionsConfig(
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-all-url"),
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-single-url"),
        defaultConfigFactory.getInt(definitionsParamsPrefix + "definitions-cached-time"),
        defaultConfigFactory.getStringList(definitionsParamsPrefix + "directions-api-keys").asScala.toList
      ),
      WebsocketConfig(
        FiniteDuration(defaultConfigFactory.getDuration("websockets.clientSendInterval").toMillis, TimeUnit.MILLISECONDS),
        defaultConfigFactory.getInt("websockets.port")
      ),
      StreamingConfig(
        FiniteDuration(defaultConfigFactory.getDuration("streaming.idleTimeBeforeVehicleDeleted").toMillis, TimeUnit.MILLISECONDS),
        defaultConfigFactory.getInt("streaming.cleanUpEveryNLines"),
      ),
      MetricsConfig(
        defaultConfigFactory.getString("metrics.host"),
        defaultConfigFactory.getInt("metrics.port"),
        defaultConfigFactory.getString("metrics.dbName"),
        defaultConfigFactory.getInt("metrics.updateInterval"),
        defaultConfigFactory.getBoolean("metrics.enabled")
      )
    )
  }
}

