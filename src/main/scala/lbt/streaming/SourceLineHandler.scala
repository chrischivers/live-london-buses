package lbt.streaming

import com.typesafe.scalalogging.StrictLogging
import lbt.StreamingConfig
import lbt.common.{Commons, Definitions}
import lbt.db.caching.{RedisArrivalTimeLog, RedisVehicleArrivalTimeLog}
import lbt.models.BusRoute

import scala.concurrent.{ExecutionContext, Future}

case class StopArrivalRecord(vehicleId: String, busRoute: BusRoute, stopIndex: Int, lastStop: Boolean)

class SourceLineHandler(redisArrivalTimeLog: RedisArrivalTimeLog, redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog, definitions: Definitions, streamingConfig: StreamingConfig)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def handle(sourceLine: SourceLine): Future[Unit] = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val busStopsFoRoute = definitions.routeDefinitions(busRoute)
    val thisBusStop = busStopsFoRoute.find(_._2.stopID == sourceLine.stopID)
      .getOrElse(throw new RuntimeException(s"No stopID found for $sourceLine in definitions"))
    val lastStop = thisBusStop._1 == busStopsFoRoute.size - 1

    val stopArrivalRecord = StopArrivalRecord(sourceLine.vehicleId, busRoute, thisBusStop._1, lastStop)

    for {
      _ <- addToRedisArrivalTimeLog(sourceLine.arrivalTimeStamp, stopArrivalRecord)
      _ <- addToRedisVehicleArrivalTimeLog(sourceLine.arrivalTimeStamp, stopArrivalRecord)
    } yield ()
  }

  private def addToRedisArrivalTimeLog(arrivalTimeStamp: Long, stopArrivalRecord: StopArrivalRecord) = {
    redisArrivalTimeLog.addArrivalRecord(arrivalTimeStamp, stopArrivalRecord)
  }

  private def addToRedisVehicleArrivalTimeLog(arrivalTimeStamp: Long, stopArrivalRecord: StopArrivalRecord) = {
    redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord, arrivalTimeStamp)
  }
}
