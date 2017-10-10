package lbt.streaming

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import cats.data.OptionT
import com.typesafe.scalalogging.StrictLogging
import lbt.StreamingConfig
import lbt.common.Definitions
import lbt.db.caching._
import lbt.metrics.MetricsLogging
import lbt.web.FilteringParams

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class CacheReadCommand(readTimeAhead: Long)

class CacheReader(redisArrivalTimeCache: RedisArrivalTimeLog, redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog, redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache, definitions: Definitions)(implicit executionContext: ExecutionContext) extends Actor with StrictLogging {

  logger.info("New cache reader created")
  override def receive = {
    case CacheReadCommand(time) =>
      val uuid = UUID.randomUUID().toString
      val startTime = System.currentTimeMillis()
      logger.info(s"Cache read command received [$uuid]")
      transferArrivalTimesToWebSocketCache(time).onComplete{
        case Success(_) => logger.info(s"Cache read command completed successfully [$uuid]. Time taken = ${System.currentTimeMillis() - startTime}")
        case Failure(e) => logger.error(s"Failure in cache read processing [$uuid]", e)
      }
    case unknown => throw new RuntimeException(s"Unknown command received by arrival time cache reader: $unknown")
  }

  def transferArrivalTimesToWebSocketCache(arrivalTimesUpTo: Long) = {
    redisArrivalTimeCache.getAndDropArrivalRecords(System.currentTimeMillis() + arrivalTimesUpTo)
      .flatMap { records =>
        MetricsLogging.incrCachedRecordsProcessed(records.size)
        for {
          transmissionDataList <- Future.sequence(records.map { case (stopArrivalRecord, timestamp) => createDataForTransmission(stopArrivalRecord, timestamp) })
          subscribersParams <- getSubscibersAndFilteringParams()
          filteringMap = subscribersParams.map {
            case (client, params) => client -> transmissionDataList.filter(data =>
              params.busRoutes.contains(data.busRoute) &&
                params.latLngBounds.isWithinBounds(data.startingBusStop.latLng))
          }
          results <- Future.sequence(filteringMap.map { case (client, recordsForTransmission) => Future.sequence(recordsForTransmission
            .map(record => redisWsClientCache.storeVehicleActivity(client, record)))
          }).map(_.flatten)
        } yield results
      }
  }

  private def createDataForTransmission(stopArrivalRecord: StopArrivalRecord, timestamp: Long) = {
    val stopList = definitions.routeDefinitions.getOrElse(stopArrivalRecord.busRoute,
      throw new RuntimeException(s"Unable to locate ${stopArrivalRecord.busRoute} in definitions file"))
    val thisStop = stopList.find { case (index, _, _) => index == stopArrivalRecord.stopIndex }
      .getOrElse(throw new RuntimeException(s"Unable to locate index ${stopArrivalRecord.stopIndex} in definitions file for route ${stopArrivalRecord.busRoute}"))
    val nextStopOpt = stopList.find { case (index, _, _) => index == stopArrivalRecord.stopIndex + 1 }

    getArrivalTimeForNextStop(stopArrivalRecord).map(nextArrivalTimeOpt => {
      BusPositionDataForTransmission(
        stopArrivalRecord.vehicleId,
        stopArrivalRecord.busRoute,
        thisStop._2,
        timestamp,
        nextStopOpt.map(_._2.stopName),
        nextArrivalTimeOpt,
        None) //todo
    })

  }

  private def getArrivalTimeForNextStop(stopArrivalRecord: StopArrivalRecord): Future[Option[Long]] = {
    redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).map(arrivalTimes => {
      arrivalTimes.find(_.stopIndex == stopArrivalRecord.stopIndex + 1).map(_.arrivalTime)
    })
  }

  private def getSubscibersAndFilteringParams(): Future[Seq[(String, FilteringParams)]] = {
    for {
      subscribers <- redisSubscriberCache.getListOfSubscribers
      filteringParams <- Future.sequence(subscribers.map(subscriber =>
        redisSubscriberCache.getParamsForSubscriber(subscriber)
          .map(_.map(fp => (subscriber, fp))))).map(_.flatten)

    } yield filteringParams
  }
}