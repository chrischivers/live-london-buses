package lbt.streaming

import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import lbt.SourceLineHandlerConfig
import lbt.common.{Commons, Definitions}
import lbt.db.caching.{BusPositionDataForTransmission, RedisDurationRecorder}
import lbt.models.{BusPolyLine, BusRoute, BusStop, LatLng}
import lbt.web.WebSocketClientHandler

import scala.concurrent.{ExecutionContext, Future}
import scalacache.{NoSerialization, ScalaCache, put, _}

class SourceLineHandler(definitions: Definitions, config: SourceLineHandlerConfig, redisDurationRecorder: RedisDurationRecorder, webSocketClientHandler: WebSocketClientHandler)(implicit scalaCache: ScalaCache[NoSerialization], executionContext: ExecutionContext) extends StrictLogging {

  type ArrivalTimestamp = Long
  type LastIndexPersisted = Int

  def handle(sourceLine: SourceLine): OptionT[Future, Unit] = {
    logger.debug(s"Handling line: $sourceLine")

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val stopList = definitions.routeDefinitions.getOrElse(busRoute, throw new RuntimeException(s"Unable to find route $busRoute in definitions after validation passed"))
    val indexOfThisStop = stopList.find(_._2.stopID == sourceLine.stopID).map(_._1).getOrElse(throw new RuntimeException(s"Unable to find stopID ${sourceLine.stopID} in stop list for route $busRoute"))
    val thisStop: (Int, BusStop, Option[BusPolyLine]) = stopList(indexOfThisStop)
    val isLastStop: Boolean = indexOfThisStop == stopList.size - 1
    val nextStopOpt = if (isLastStop) None else Some(stopList(indexOfThisStop + 1))

    def updateTimeDifferenceForStop(): OptionT[Future, Unit] = {
      logger.debug(s"Updating stop cache for $sourceLine")
      for {
        _ <- OptionT.liftF(put(sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfThisStop)(sourceLine.arrival_TimeStamp, ttl = Some(config.cacheTtl)))
        previousStopTime <- OptionT(get[ArrivalTimestamp, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfThisStop - 1))
        timeDiff = sourceLine.arrival_TimeStamp - previousStopTime
        _ <- if (timeDiff >= config.minimumTimeDifferenceToPersist.toMillis) persistTimeDifference(previousStopTime, timeDiff)
            else OptionT.liftF(Future.successful(())) //ignored if time difference below threshold
      } yield ()
    }

    def persistTimeDifference(previousStopArrivalTime: Long, timeDiff: Long) = {
      for {
        _ <- OptionT.liftF(redisDurationRecorder.persistStopToStopTime(busRoute, indexOfThisStop - 1, indexOfThisStop, previousStopArrivalTime, (timeDiff / 1000).toInt))
        _ <- OptionT.liftF(remove(sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfThisStop - 1))
        _ <- OptionT.liftF(put(sourceLine.vehicleID, sourceLine.route, sourceLine.direction)(indexOfThisStop, ttl = Some(config.cacheTtl)))
      } yield ()
    }

    def sendToWebSocketHandler(busPositionDataForTransmission: BusPositionDataForTransmission) = {
      OptionT.liftF(webSocketClientHandler.queueTransmissionDataToClients(busPositionDataForTransmission))
    }

    def toTransmissionData(sourceLine: SourceLine, averageTimeToNextStopOpt: Option[Int]): BusPositionDataForTransmission = {
      BusPositionDataForTransmission(
        sourceLine.vehicleID,
        busRoute,
        thisStop._2,
        sourceLine.arrival_TimeStamp,
        nextStopOpt.map(_._2.stopName),
        averageTimeToNextStopOpt,
        thisStop._3.map(_.toMovementInstructions))
    }

    def getAverageTimeToNextStop: Future[Option[Double]] = {
      if (!isLastStop) redisDurationRecorder.getStopToStopAverageTime(busRoute, indexOfThisStop, indexOfThisStop + 1).map(Some(_))
      //TODO handle empty list coming back (i.e. no duration data in db)
      else Future.successful(None)
    }

    OptionT.liftF(get[LastIndexPersisted, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction)).flatMap {
      case Some(lastIndexCached) if indexOfThisStop <= lastIndexCached => OptionT.liftF(Future.successful(())) //disregard has already gone
      case _ =>
        for {
          _ <- updateTimeDifferenceForStop().orElse(OptionT.liftF(Future.successful()))
          avg <- OptionT.liftF(getAverageTimeToNextStop)
          _ <- sendToWebSocketHandler(toTransmissionData(sourceLine, avg.map(_.toInt)))
        } yield ()
    }


  }
}
