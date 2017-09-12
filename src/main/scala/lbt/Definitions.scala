package lbt

import lbt.db.RouteDefinitionsTable
import lbt.models.{BusRoute, BusStop}
import scala.concurrent.duration._
import scala.concurrent.Await

class Definitions(routeDefinitionsTable: RouteDefinitionsTable ) {

  lazy val routeDefinitions: Map[BusRoute, List[(Int, BusStop)]] = {
   Await.result(routeDefinitionsTable.getAllRouteDefinitions, 5 minutes)
  }

}
