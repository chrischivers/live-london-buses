package lbt.db

import com.github.mauricio.async.db.{Connection, QueryResult}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

trait Table[T <: Connection] extends StrictLogging {
  val db: DB[T]
  val schema: Schema

  protected def createTable: Future[QueryResult]

  def dropTable(implicit executor: ExecutionContext): Future[Unit] = {
    logger.info(s"Dropping ${schema.tableName}")
    val query = s"DROP TABLE IF EXISTS ${schema.tableName}"
    db.connectionPool.sendQuery(query).map(_ => ())
  }
}
