package lbt.streaming

import cats.data.Validated.{Invalid, Valid, invalid, valid}
import cats.data.{Validated, NonEmptyList => NEL, _}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Commons
import lbt.models.{BusRoute, BusStop}


case class SourceLine(route: String, direction: Int, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)

object SourceLine extends StrictLogging {

  def fromRawLine(line: String): Option[SourceLine] = {
    def splitLine(line: String) = line
      .substring(1, line.length - 1) // remove leading and trailing square brackets,
      .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
      .map(_.replaceAll("\"", "")) //takes out double quotations after split
      .map(_.trim) // remove trailing or leading white space
      .tail // discards the first element (always '1')

    def arrayCorrectLength(array: Array[String]): Boolean = array.length == 6

    val split = splitLine(line)

    if (arrayCorrectLength(split)) Some(SourceLine(split(1).toUpperCase, split(2).toInt, split(0), split(3), split(4), split(5).toLong))
    else {
      logger.error(s"Source array has incorrect number of elements (${split.length}. 6 expected. Or invalid web page retrieved \n " + split.mkString(","))
      None
    }
  }

  def validate(sourceLine: SourceLine): Boolean = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

    def validRouteAndStop(busRoute: BusRoute): Validated[NEL[String], String] = {
      //TODO look up in definitions
      //      definitions.get(busRoute) match {
      //        case Some(stopList) => validStop(stopList)
      //        case None => s"Route not defined in definitions. Route ID: ${busRoute.name}. Direction: ${busRoute.direction}".failureNel
      //      }
      valid(s"Bus Route $busRoute is valid")
    }

    def validStop(busStopList: List[BusStop]): Validated[NEL[String], String] = {
      //TODO look up stop definitions
      //      busStopList.find(stop => stop.stopID == sourceLine.stopID) match {
      //        case Some(busStop) => busStop.successNel
      //        case None => s"Bus Stop ${sourceLine.stopID} not defined in definitions for route ${sourceLine.route} and direction ${sourceLine.direction}".failureNel
      //      }
      valid(s"Bus stops list for route $busRoute are valid")
    }

    def notOnIgnoreList(): Validated[NEL[String], String] = {
      //TODO
      valid(s"Bus route $busRoute is not on the ignore list")
    }

    def isInPast(): Validated[NEL[String], String] = {
      //TODO is this working with clock change?
      if (sourceLine.arrival_TimeStamp - System.currentTimeMillis() > 0) valid("Event is not in the past")
      else invalid(NEL.of("Event is in the past"))
    }

    (validRouteAndStop(busRoute) |@| validStop(List.empty) |@| notOnIgnoreList() |@| isInPast()).map(_ + _ + _ + _) match {
      case Valid(v) => true
      case Invalid(iv) =>
        logger.debug(s"Unable to validate sourceLine $sourceLine, errors: ${iv.toString()}")
        false
    }
  }
}
