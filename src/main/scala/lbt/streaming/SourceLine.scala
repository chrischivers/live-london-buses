package lbt.streaming

import cats.data.Validated.{Invalid, Valid, invalid, valid}
import cats.data.{Validated, NonEmptyList => NEL, _}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import lbt.common.{Commons, Definitions}
import lbt.models.{BusRoute, BusStop}


case class SourceLine(route: String, direction: Int, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long) {
  def validate(definitions: Definitions): Boolean = SourceLine.validate(this, definitions)
}

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

  def validate(sourceLine: SourceLine, definitions: Definitions): Boolean = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

    def validRoute(): Validated[NEL[String], String] = {
      definitions.routeDefinitions.get(busRoute) match {
        case None => invalid(NEL.of(s"Bus route $busRoute does not exist in definitions"))
        case Some(_) => valid(s"Bus Route $busRoute is valid")
      }
    }

    def validStop(): Validated[NEL[String], String] = {
      definitions.routeDefinitions.get(busRoute) match {
        case Some(list) if list.exists(_._2.stopID == sourceLine.stopID) => valid("Bus stop is valid")
        case _ => invalid(NEL.of(s"Bus stop not found for bus route $busRoute in definitions"))
      }
    }

    def notOnIgnoreList(): Validated[NEL[String], String] = {
      //TODO
      valid(s"Bus route $busRoute is not on the ignore list")
    }

    def isInPast(): Validated[NEL[String], String] = {
      if (sourceLine.arrival_TimeStamp - System.currentTimeMillis() > 0) valid("Event is not in the past")
      else invalid(NEL.of("Event is in the past"))
    }

    def isTooFarInFuture() = ???

    (validRoute() |@| validStop() |@| notOnIgnoreList() |@| isInPast()).map(_ + _ + _ + _) match {
      case Valid(_) => true
      case Invalid(iv) =>
        logger.debug(s"Unable to validate sourceLine $sourceLine, errors: ${iv.toString()}")
        false
    }
  }
}
