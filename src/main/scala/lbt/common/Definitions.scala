package lbt.common

import com.typesafe.scalalogging.StrictLogging
import lbt.common.Commons.BusPolyLine
import lbt.db.RouteDefinitionsTable
import lbt.models.{BusRoute, BusStop}

import scala.concurrent.Await
import scala.concurrent.duration._

class Definitions(routeDefinitionsTable: RouteDefinitionsTable) extends StrictLogging {

  lazy val routeDefinitions: Map[BusRoute, List[(Int, BusStop, Option[BusPolyLine])]] = {
    val result = Await.result(routeDefinitionsTable.getAllRouteDefinitions, 5 minutes)
    logger.info(s"Obtained ${result.size} route definitions from DB")
    result
  }

}
