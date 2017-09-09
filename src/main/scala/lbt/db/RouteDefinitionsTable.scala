package lbt.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import lbt.models.{BusRoute, BusStop}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


class RouteDefinitionsTable(val db: SqlDb[PostgreSQLConnection], val schema: RouteDefinitionSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

  if (createNewTable) {
    Await.result({
      for {
        _ <- dropTable
        newTable <- createTable
      } yield newTable
    }, 1 minute) //Blocks while table created

  }

  override def createTable: Future[QueryResult] = {
    logger.info(s"Creating Table ${schema.tableName}")
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendQuery(
        s"""
           |CREATE TABLE IF NOT EXISTS
           |${schema.tableName} (
           |    ${schema.routeId} varchar NOT NULL,
           |    ${schema.direction} varchar NOT NULL,
           |    ${schema.sequence} integer NOT NULL,
           |    ${schema.stopId} varchar NOT NULL,
           |    ${schema.stopName} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertRouteDefinitions(route: BusRoute, stopSeq: List[(BusStop, Int)]): Future[List[QueryResult]] = {
    Future.sequence(stopSeq.map {case (stop, index) =>
      insertRouteDefinition(route, stop, index)
    })
  }

  private def insertRouteDefinition(route: BusRoute, stop: BusStop, stopSeqNo: Int): Future[QueryResult] = {
    val statement =
      s"INSERT INTO ${schema.tableName} " +
        s"(${schema.routeId}, ${schema.direction}, ${schema.sequence}, " +
        s"${schema.stopId}, ${schema.stopName}, ${schema.lat}, " +
        s"${schema.lng}, ${schema.lastUpdated}) " +
        "VALUES (?,?,?,?,?,?,?,'now')"

    db.connectionPool.sendPreparedStatement(statement,
      List(route.id, route.direction, stopSeqNo,
        stop.stopID, stop.stopName, stop.latitude, stop.longitude))
  }

  def getStopSequenceFor(route: BusRoute): Future[List[(Int, BusStop)]] = {
    val query =
      s"SELECT * " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${schema.routeId} = ? " +
        s"AND ${schema.direction} = ? " +
        s"ORDER BY ${schema.sequence}"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(route.id, route.direction))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val id = res(schema.stopId).asInstanceOf[String]
          val name = res(schema.stopName).asInstanceOf[String]
          val lat = res(schema.lat).asInstanceOf[Float].toDouble
          val lng = res(schema.lng).asInstanceOf[Float].toDouble
          val sequenceNo = res(schema.sequence).asInstanceOf[Int]
          (sequenceNo, BusStop(id, name, lat, lng))
        }).toList
        case None => List.empty
      }
    }
  }

  def getAllRouteDefinitions: Future[Map[BusRoute, List[(Int, BusStop)]]] = {
    val query =
      s"SELECT * " +
        s"FROM ${schema.tableName} " +
        s"ORDER BY ${schema.routeId} ASC, ${schema.direction} ASC, ${schema.sequence} ASC"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query)
    } yield {
      val results = queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val routeId = res(schema.routeId).asInstanceOf[String]
          val direction = res(schema.direction).asInstanceOf[String]
          val id = res(schema.stopId).asInstanceOf[String]
          val name = res(schema.stopName).asInstanceOf[String]
          val lat = res(schema.lat).asInstanceOf[Float].toDouble
          val lng = res(schema.lng).asInstanceOf[Float].toDouble
          val sequenceNo = res(schema.sequence).asInstanceOf[Int]
          (BusRoute(routeId, direction), (sequenceNo, BusStop(id, name, lat, lng)))
        }).toList
        case None => List.empty
      }
    val uniqueRoutesList = results.map(_._1).distinct
    uniqueRoutesList.map(route => route -> results.filter(_._1 == route).map(_._2).sortBy(_._1)).toMap
    }
  }
}