package lbt

import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.syntax._
import lbt.common.Definitions
import lbt.db.caching.BusPositionDataForTransmission
import lbt.models._
import lbt.streaming.{SourceLine, StopArrivalRecord}
import lbt.web.FilteringParams
import lbt.web.MapsHttpService.NextStopResponse
import org.scalatest.OptionValues

import scala.collection.mutable.ListBuffer
import scala.util.Random

trait SharedTestFeatures extends OptionValues {

  implicit val busRouteDecoder: Decoder[BusRoute] = deriveDecoder[BusRoute]
  implicit val busStopDecoder: Decoder[BusStop] = deriveDecoder[BusStop]
  implicit val busPosDataDecoder: Decoder[BusPositionDataForTransmission] = deriveDecoder[BusPositionDataForTransmission]

  def createBusPositionData(vehicleId: String = Random.nextString(10),
                            busRoute: BusRoute = BusRoute("3", "outbound"),
                            busStop: BusStop = BusStop("490003059E", "Abingdon Street", LatLng(51.49759, -0.125605)),
                            isPenultimateStop: Boolean = false,
                            nextStopNameOpt: Option[String] = Some("NextStop"),
                            nextStopIndexOpt: Option[Int] = Some(5),
                            arrivalTimeStamp: Long = System.currentTimeMillis(),
                            arrivalTimeAtNextStop: Option[Long] = Some(System.currentTimeMillis() + 120000),
                            movementInstructionsOpt: Option[List[MovementInstruction]] = Some(BusPolyLine("}biyHzoW~KqA").toMovementInstructions),
                            destination: String = "Crystal Palace") = {
    BusPositionDataForTransmission(vehicleId, busRoute, arrivalTimeStamp, busStop.latLng, isPenultimateStop, nextStopNameOpt, nextStopIndexOpt, arrivalTimeAtNextStop, movementInstructionsOpt, destination)
  }

  def createFilteringParams(busRoutes: Option[List[BusRoute]] = Some(List(BusRoute("3", "outbound"))),
                            latLngBounds: LatLngBounds = LatLngBounds(LatLng(50, -1), LatLng(52, 1))) = {
    FilteringParams(busRoutes, latLngBounds)
  }

  def parsePacketsReceived(msgs: ListBuffer[String]): List[BusPositionDataForTransmission] = {
    msgs.flatMap(parseWebsocketCacheResult).flatten.toList
  }

  def parseWebsocketCacheResult(str: String): Option[List[BusPositionDataForTransmission]] = {
    parse(str).toOption.flatMap(_.as[List[BusPositionDataForTransmission]].toOption)
  }

  def parseNextStopsJson(str: String): Option[List[NextStopResponse]] = {
    parse(str).toOption.flatMap(_.as[List[NextStopResponse]].toOption)
  }

  def createJsonStringFromFilteringParams(filteringParams: FilteringParams): String = {
    filteringParams.asJson.noSpaces
  }

  def generateSourceLine(
                          route: String = "25",
                          direction: Int = 1,
                          stopId: String = "490007497E",
                          destination: String = "Ilford",
                          vehicleId: String = "BJ11DUV",
                          timeStamp: Long = System.currentTimeMillis() + 30000) = {
    SourceLine(route, direction, stopId, destination, vehicleId, timeStamp)
  }


  def generateStopArrivalRecord(
                                 vehicleId: String = "BJ11DUV",
                                 busRoute: BusRoute = BusRoute("25", "outbound"),
                                 stopIndex: Int = 7,
                                 destinationText: String = "Ilford",
                                 lastStop: Boolean = false) = {
    StopArrivalRecord(vehicleId, busRoute, stopIndex, destinationText, lastStop)
  }

  def getBusStopFromStopID(stopId: String, definitions: Definitions): Option[BusStop] = {
    definitions.routeDefinitions.values.flatten.find(_._2.stopID == stopId).map(_._2)
  }

  def getNextBusStopFromStopID(stopId: String, busRoute: BusRoute, definitions: Definitions): Option[BusStop] = {
    val thisStopIndex = definitions.routeDefinitions(busRoute).find(_._2.stopID == stopId).map(_._1)
    definitions.routeDefinitions(busRoute).find(_._1 == thisStopIndex.get + 1).map(_._2)
  }

  def getNextBusStopIndexFromStopID(stopId: String, busRoute: BusRoute, definitions: Definitions): Option[Int] = {
    val thisStopIndex = definitions.routeDefinitions(busRoute).find(_._2.stopID == stopId).map(_._1)
    definitions.routeDefinitions(busRoute).find(_._1 == thisStopIndex.get + 1).map(_._1)
  }
}
