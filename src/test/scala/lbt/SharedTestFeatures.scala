package lbt

import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.syntax._
import lbt.db.caching.BusPositionDataForTransmission
import lbt.models._
import lbt.streaming.SourceLine
import lbt.web.FilteringParams
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
                            nextStopNameOpt: Option[String] = Some("NextStop"),
                            arrivalTimeStamp: Long = System.currentTimeMillis(),
                            durationToNextStopOpt: Option[Int] = Some(100),
                            movementInstructionsOpt: Option[List[MovementInstruction]] = Some(BusPolyLine("}biyHzoW~KqA").toMovementInstructions)) = {
    BusPositionDataForTransmission(vehicleId, busRoute, busStop, arrivalTimeStamp, nextStopNameOpt, durationToNextStopOpt, movementInstructionsOpt)
  }

  def createFilteringParams(busRoutes: List[BusRoute] = List(BusRoute("3", "outbound")),
                                    latLngBounds: LatLngBounds = LatLngBounds(LatLng(50,-1), LatLng(52,1))) = {
    FilteringParams(busRoutes, latLngBounds)
  }

  def parsePacketsReceived(msgs: ListBuffer[String]): List[BusPositionDataForTransmission] = {
    msgs.flatMap(parseWebsocketCacheResult).flatten.toList
  }

  def parseWebsocketCacheResult(str: String): Option[List[BusPositionDataForTransmission]] = {
    parse(str).toOption.flatMap(_.as[List[BusPositionDataForTransmission]].toOption)
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

}
