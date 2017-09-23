package lbt.scripts

import com.typesafe.scalalogging.StrictLogging
import lbt.common.Commons.BusPolyLine
import lbt.common.Definitions
import lbt.db.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusStop
import lbt.{ConfigLoader, DefinitionsConfig}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random
import scala.xml.XML

object BusRoutePolyLineUpdater extends App with StrictLogging{
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)

  val busRoutePolyLineUpdater = new BusRoutePolyLineUpdater(config.definitionsConfig, routeDefinitionsTable)
  busRoutePolyLineUpdater.start
}

class BusRoutePolyLineUpdater(definitionsConfig: DefinitionsConfig, routeDefinitionsTable: RouteDefinitionsTable) extends StrictLogging {

  val definitions = new Definitions(routeDefinitionsTable)

  def start: Unit = {
    definitions.routeDefinitions.foreach { routeDef =>
      val stopsWithNoPolyLines = routeDef._2.filter(_._3.isEmpty)
      stopsWithNoPolyLines.foreach { fromStop =>
        for {
          toStop <- routeDef._2.find(_._1 == fromStop._1 + 1)
          polyLine <- getPolyLineFor(fromStop._2, toStop._2)
        } yield Await.result(routeDefinitionsTable.updatePolyLine(routeDef._1, fromStop._1, polyLine), 5 minutes)
      }
    }
  }


  def getPolyLineFor(fromStop: BusStop, toStop: BusStop): Option[BusPolyLine] = {

    //TODO throttle requests

    val now = DateTime.now()
    val todayAt10am = new DateTime(now.year().get(), now.monthOfYear().get(), now.dayOfMonth().get(), 10, 0)
    val apiKey = Random.shuffle(definitionsConfig.directionsApiKeys).head

    val polyLineUrl = s"https://maps.googleapis.com/maps/api/directions/xml?" +
      s"origin=${fromStop.latitude},${fromStop.longitude}" +
      s"&destination=${toStop.latitude},${toStop.longitude}" +
      s"&key=$apiKey" +
      s"&mode=transit" +
      s"&transit_mode=bus" +
      s"&departure_time=${todayAt10am.getMillis / 1000}"

    logger.info(s"Using URL: $polyLineUrl")

    val xml = XML.load(polyLineUrl)

    (xml \\ "DirectionsResponse" \ "status").text match {
      case "OK" =>
        val polyLine = (xml \\ "DirectionsResponse" \\ "route" \\ "overview_polyline" \ "points").text
        if (polyLine.length > 0) Some(polyLine) else None
      case "OVER_QUERY_LIMIT" =>
        Thread.sleep(5000)
        getPolyLineFor(fromStop, toStop)
      case unknown => throw new RuntimeException(s"Unknown error retrieving polyline from URL $polyLineUrl, status code: $unknown")
    }
  }
}
