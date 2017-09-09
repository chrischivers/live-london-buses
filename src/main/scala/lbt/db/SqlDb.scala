package lbt.db

import com.github.mauricio.async.db.pool.{ConnectionPool, ObjectFactory, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.{Configuration, Connection}
import com.typesafe.scalalogging.StrictLogging
import lbt.PostgresDBConfig

import scala.concurrent.Future
import scala.concurrent.duration._

trait SqlDb[T <: Connection] extends StrictLogging {

  val connectionConfiguration: Configuration

  val connectionPoolConfig: PoolConfiguration

  val connection: ObjectFactory[T]

  val connectionPool: ConnectionPool[T]

  def connectToDB: Future[Connection] = connectionPool.connect

  def disconnect: Future[Connection] = connectionPool.disconnect


}

class PostgresDB(dBConfig: PostgresDBConfig) extends SqlDb[PostgreSQLConnection] {
  logger.info(s"Setting up DB: ${dBConfig.dbName}")

  override val connectionConfiguration = Configuration(
  username = dBConfig.username,
  password = Some(dBConfig.password),
  host = dBConfig.host,
  port = dBConfig.port,
  database = Some(dBConfig.dbName),
  connectTimeout = 120.seconds,
  testTimeout = 120.seconds)

  override val connectionPoolConfig: PoolConfiguration = new PoolConfiguration(maxObjects = 5, maxIdle = 5000, maxQueueSize = 100000)
  override val connection: ObjectFactory[PostgreSQLConnection] = new PostgreSQLConnectionFactory(connectionConfiguration)
  override val connectionPool: ConnectionPool[PostgreSQLConnection] = new ConnectionPool[PostgreSQLConnection](connection, connectionPoolConfig)
}