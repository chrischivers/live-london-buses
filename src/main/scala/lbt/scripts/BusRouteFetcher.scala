package lbt.scripts

import com.typesafe.scalalogging.StrictLogging
import io.circe
import io.circe.parser._
import lbt.ConfigLoader
import lbt.JsonCodecs._
import lbt.db.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusRoute, BusStop}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object BusRouteFetcher extends App with StrictLogging {

  val config = ConfigLoader.defaultConfig
  val defConfig = config.definitionsConfig
  val db = new PostgresDB(config.dBConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)
  logger.info("Refreshing bus route definitions from web")

  val result = getRouteList match {
    case Left(e) => throw e
    case Right(routes) =>
      val busRoutes = routes.filter(_.mode == "bus").flatMap(r => r.directions.map(dir => BusRoute(r.routeId, dir)))
        .filter(_.id == "3") //TODO open to more routes

    val numberToProcess = busRoutes.size
    logger.info(s"Bus route fetcher has $numberToProcess to process")

    busRoutes.zipWithIndex.map { case (route, routeIndex) =>
      logger.info(s"Processing $routeIndex of $numberToProcess. (Route: ${route.id}, Direction: ${route.direction}")
      getStopListFor(route) match {
        case Left(e) => throw e
        case Right(stopList) => routeDefinitionsTable.insertRouteDefinitions(route, stopList.zipWithIndex)
      }
    }
  }
  Await.result(Future.sequence(result), 30 minutes)


  def getRouteList: Either[circe.Error, List[JsonRoute]] = {
    val allRouteJsonDataRaw = Source.fromURL(defConfig.sourceAllUrl).mkString
    val updatedRouteList = parse(allRouteJsonDataRaw)
    for {
      json <- updatedRouteList
      decodedRoutes <- json.as[List[JsonRoute]]
    } yield {
      decodedRoutes
    }
  }


  def getStopListFor(busRoute: BusRoute): Either[circe.Error, List[BusStop]] = {
    val url = defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.id).replace("#Direction#", busRoute.direction)
    for {
      json <- parse(Source.fromURL(url).mkString)
      decodedStops <- json.as[List[BusStop]]
    } yield {
      decodedStops
    }
  }

}
