package lbt.streaming

import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import lbt.SourceLineHandlerConfig
import lbt.common.{Commons, Definitions}
import lbt.db.caching.RedisDurationRecorder
import lbt.models.BusRoute

import scala.concurrent.{ExecutionContext, Future}
import scalacache.{NoSerialization, ScalaCache, put, _}

class SourceLineHandler(definitions: Definitions, config: SourceLineHandlerConfig, redisDurationRecorder: RedisDurationRecorder)(implicit scalaCache: ScalaCache[NoSerialization], executionContext: ExecutionContext) extends StrictLogging {

  type ArrivalTimestamp = Long
  type LastIndexPersisted = Int

  def handle(sourceLine: SourceLine): OptionT[Future, Unit] = {
    logger.debug(s"Handling line: $sourceLine")

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val stopList = definitions.routeDefinitions.getOrElse(busRoute, throw new RuntimeException(s"Unable to find route $busRoute in definitions after validation passed"))
    val indexOfStop = stopList.find(_._2.stopID == sourceLine.stopID).map(_._1).getOrElse(throw new RuntimeException(s"Unable to find stopID ${sourceLine.stopID} in stop list for route $busRoute"))


    def updateStop: OptionT[Future, Unit] = {
      logger.debug("Updating stop cache")
      for {
        _ <- OptionT.liftF(put(sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfStop)(sourceLine.arrival_TimeStamp, ttl = Some(config.cacheTtl)))
        previousStopTime <- OptionT(get[ArrivalTimestamp, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfStop - 1))
        timeDiff = sourceLine.arrival_TimeStamp - previousStopTime
        _ <- if (timeDiff >= config.minimumTimeDifferenceToPersist.toMillis)  persistTimeDifference(previousStopTime, timeDiff)
            else OptionT.liftF(Future.successful(())) //ignored if time difference below threshold
      } yield ()
    }

    def persistTimeDifference(previousStopArrivalTime: Long, timeDiff: Long) = {
      for {
        _ <- OptionT.liftF(redisDurationRecorder.persistStopToStopTime(busRoute, indexOfStop - 1, indexOfStop, previousStopArrivalTime, (timeDiff / 1000).toInt))
        _ <- OptionT.liftF(remove(sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfStop - 1))
        _ <- OptionT.liftF(put(sourceLine.vehicleID, sourceLine.route, sourceLine.direction)(indexOfStop, ttl = Some(config.cacheTtl)))
      } yield ()
    }

    OptionT.liftF(get[LastIndexPersisted, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction)).flatMap {
      case Some(lastIndexCached) if indexOfStop <= lastIndexCached => OptionT.liftF(Future.successful(())) //disregard
      case _ => updateStop
    }

  }
}
