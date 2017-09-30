package lbt.common

import com.typesafe.scalalogging.StrictLogging
import lbt.db.sql.RouteDefinitionsTable
import lbt.models.{BusPolyLine, BusRoute, BusStop}

import scala.concurrent.Await
import scala.concurrent.duration._

class Definitions(routeDefinitionsTable: RouteDefinitionsTable) extends StrictLogging {

  lazy val routeDefinitions: Map[BusRoute, List[(Int, BusStop, Option[BusPolyLine])]] = {
    val result = Await.result(routeDefinitionsTable.getAllRouteDefinitions, 5 minutes)
    logger.info(s"Obtained ${result.size} route definitions from DB")
    result
  }

}
