package lbt.scripts

import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.{ConfigLoader, DefinitionsConfig}
import lbt.models.{BusRoute, BusStop}
import lbt.scripts.BusRouteDefinitionsUpdater.config
import org.joda.time.DateTime

import scala.io.Source
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.xml.XML

object BusRoutePolyLineUpdater extends App {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(), createNewTable = false)
  val definitions = new Definitions(routeDefinitionsTable)

  val route = definitions.routeDefinitions(BusRoute("3", "outbound"))
  val updater = new BusRoutePolyLineUpdater(config.definitionsConfig)
  val polyLine = updater.getPolyLineFor(route(3)._2, route(4)._2)
  println(polyLine)

}

class BusRoutePolyLineUpdater(definitionsConfig: DefinitionsConfig) extends StrictLogging {

  type BusPolyLine = String

  def start = {

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
