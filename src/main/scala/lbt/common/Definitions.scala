package lbt.common

import lbt.db.RouteDefinitionsTable
import lbt.models.{BusRoute, BusStop}

import scala.concurrent.Await
import scala.concurrent.duration._

class Definitions(routeDefinitionsTable: RouteDefinitionsTable ) {

  lazy val routeDefinitions: Map[BusRoute, List[(Int, BusStop)]] = {
   Await.result(routeDefinitionsTable.getAllRouteDefinitions, 5 minutes)
  }

}
