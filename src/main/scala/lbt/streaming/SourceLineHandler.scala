package lbt.streaming

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.StrictLogging
import lbt.common.{Commons, Definitions}
import lbt.db.caching.RedisArrivalTimeLog
import lbt.models.BusRoute

case class StopArrivalRecord(vehicleId: String, busRoute: BusRoute, stopIndex: Int)

class SourceLineHandler(redisArrivalTimeLog: RedisArrivalTimeLog, definitions: Definitions)(implicit actorSystem: ActorSystem) extends StrictLogging {

  val vehicleCoordinator = actorSystem.actorOf(Props(new VehicleCoordinator(definitions)))

  def handle(sourceLine: SourceLine) = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val busStop = definitions.routeDefinitions(busRoute).find(_._2.stopID == sourceLine.stopID)
      .getOrElse(throw new RuntimeException(s"No stopID found for $sourceLine in definitions"))

    val stopArrivalRecord = StopArrivalRecord(sourceLine.vehicleId, busRoute, busStop._1)

    vehicleCoordinator ! Handle(stopArrivalRecord, sourceLine.arrivalTimeStamp)
    addToRedisArrivalTimeLog(sourceLine.arrivalTimeStamp, stopArrivalRecord)
  }


  private def addToRedisArrivalTimeLog(arrivalTimeStamp: Long, stopArrivalRecord: StopArrivalRecord) = {
    logger.debug(s"Adding $stopArrivalRecord to Redis arrival time log")
    redisArrivalTimeLog.addArrivalRecord(arrivalTimeStamp, stopArrivalRecord)
  }
}
