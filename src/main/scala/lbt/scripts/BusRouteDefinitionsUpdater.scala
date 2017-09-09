package lbt.scripts

import com.github.mauricio.async.db.QueryResult
import com.typesafe.scalalogging.StrictLogging
import io.circe
import io.circe.parser._
import lbt.{ConfigLoader, DefinitionsConfig}
import lbt.JsonCodecs._
import lbt.db.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusRoute, BusStop}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object BusRouteDefinitionsUpdater extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val defConfig = config.definitionsConfig
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)

  val updater = new BusRouteDefinitionsUpdater(defConfig, routeDefinitionsTable)
  logger.info("Starting definitions update")
  Await.result(updater.start(Some(List(BusRoute("3", "outbound")))), 120 minutes)
  logger.info("Finished updating definitions")
}


class BusRouteDefinitionsUpdater(defConfig: DefinitionsConfig, routeDefinitionsTable: RouteDefinitionsTable) extends StrictLogging {

  def start(limitUpdateTo: Option[List[BusRoute]] = None): Future[List[QueryResult]] = {
    logger.info("Refreshing bus route definitions from web")
    val result = getRouteList match {
      case Left(e) => throw e
      case Right(routes) =>
        val allBusRoutes = routes.filter(_.mode == "bus").flatMap(r => r.directions.map(dir => BusRoute(r.routeId, dir)))
        logger.info(s"${allBusRoutes.size} bus routes in total")
        val filteredBusRoutes = limitUpdateTo.fold(allBusRoutes)(limitBy => allBusRoutes.filter(route => limitBy.contains(route)))
        logger.info(s"${filteredBusRoutes.size} bus routes after filtering applied")

        val numberToProcess = filteredBusRoutes.size
        logger.info(s"Bus route fetcher has $numberToProcess to process")

        filteredBusRoutes.zipWithIndex.map { case (route, routeIndex) =>
          logger.info(s"Processing $routeIndex of $numberToProcess (Route: ${route.id}, Direction: ${route.direction})")
          getStopListFor(route) match {
            case Left(e) => throw e
            case Right(stopList) => routeDefinitionsTable.insertRouteDefinitions(route, stopList.zipWithIndex)
          }
        }
    }
    Future.sequence(result).map(_.flatten)
  }


  private def getRouteList: Either[circe.Error, List[JsonRoute]] = {
    val allRouteJsonDataRaw = Source.fromURL(defConfig.sourceAllUrl).mkString
    val updatedRouteList = parse(allRouteJsonDataRaw)
    for {
      json <- updatedRouteList
      decodedRoutes <- json.as[List[JsonRoute]]
    } yield {
      decodedRoutes
    }
  }


  private def getStopListFor(busRoute: BusRoute): Either[circe.Error, List[BusStop]] = {
    val url = defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.id).replace("#Direction#", busRoute.direction)
    for {
      json <- parse(Source.fromURL(url).mkString)
      decodedStops <- json.as[List[BusStop]]
    } yield {
      decodedStops
    }
  }

}