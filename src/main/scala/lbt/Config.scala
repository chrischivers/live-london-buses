package lbt

import com.typesafe.config.ConfigFactory

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, linesToDisregard: Int, waitTimeAfterClose: Int, cacheTimeToLiveSeconds: Int, timeWindowToAcceptLines: Int, numberEmptyIteratorCasesBeforeRestart: Int, simulationIterator: Option[Iterator[String]] = None)

case class DefinitionsConfig(sourceAllUrl: String, sourceSingleUrl: String, definitionsCachedTime: Int)

sealed trait DBConfig {
  val host: String
  val port: Int
  val username: String
  val password: String
  val dbName: String
}

case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig

case class RedisDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig

case class HistoricalRecordsConfig(vehicleInactivityTimeBeforePersist: Long, numberOfLinesToCleanupAfter: Int, minimumNumberOfStopsToPersist: Int, toleranceForFuturePredictions: Long, defaultRetrievalLimit: Int)

case class LBTConfig(
                      dataSourceConfig: DataSourceConfig,
                      postgresDbConfig: PostgresDBConfig,
                      redisDBConfig: RedisDBConfig,
                      definitionsConfig: DefinitionsConfig,
                      historicalRecordsConfig: HistoricalRecordsConfig)

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val dataBaseParamsPrefix = "database."
    val definitionsParamsPrefix = "dataSource.definitions."
    val historicalRecordsParamsPrefix = "lbt.historical-records."
    val postgresDBParamsPrefix = "db.postgres."
    val redisDBParamsPrefix = "db.redis."
    LBTConfig(
      DataSourceConfig(
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "tfl-url"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "username"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "password"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "authscope-url"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "authscope-port"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "connection-timeout"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "number-lines-disregarded"),
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
      RedisDBConfig(
        defaultConfigFactory.getString(redisDBParamsPrefix + "host"),
        defaultConfigFactory.getInt(redisDBParamsPrefix + "port"),
        defaultConfigFactory.getString(redisDBParamsPrefix + "username"),
        defaultConfigFactory.getString(redisDBParamsPrefix + "password"),
        defaultConfigFactory.getString(redisDBParamsPrefix + "dbName")
      ),
      DefinitionsConfig(
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-all-url"),
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-single-url"),
        defaultConfigFactory.getInt(definitionsParamsPrefix + "definitions-cached-time")
      ),
      HistoricalRecordsConfig(
        defaultConfigFactory.getLong(historicalRecordsParamsPrefix + "vehicle-inactivity-time-before-persist"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "lines-to-cleanup-after"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "minimum-number-of-stops-for-persist"),
        defaultConfigFactory.getLong(historicalRecordsParamsPrefix + "tolerance-for-future-predictions"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "default-retrieval-limit")
      )
    )

  }
}

