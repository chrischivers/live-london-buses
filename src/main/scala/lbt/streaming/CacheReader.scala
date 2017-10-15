package lbt.streaming

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.caching._
import lbt.metrics.MetricsLogging
import lbt.web.FilteringParams

import scala.concurrent.{ExecutionContext, Future}

case class CacheReadCommand(readTimeAhead: Long)

class CacheReader(redisArrivalTimeCache: RedisArrivalTimeLog, redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog, redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache, definitions: Definitions)(implicit executionContext: ExecutionContext) extends Actor with StrictLogging {

 type ClientId = String

  override def receive = {
    case CacheReadCommand(time) =>
      MetricsLogging.measureCacheReadProcess {
        transferArrivalTimesToWebSocketCache(time)
      }
    case unknown => throw new RuntimeException(s"Unknown command received by arrival time cache reader: $unknown")
  }

  def transferArrivalTimesToWebSocketCache(arrivalTimesUpTo: Long) = {
    redisArrivalTimeCache.getAndDropArrivalRecords(System.currentTimeMillis() + arrivalTimesUpTo)
      .flatMap { allRecords =>
        MetricsLogging.incrCachedRecordsProcessed(allRecords.size)
        val filteredRecords = allRecords.filter(_._1.lastStop != true) //disregard last stops as these are not sent to client
        Future.sequence(filteredRecords.map { case (stopArrivalRecord, timestamp) => createDataForTransmission(stopArrivalRecord, timestamp) }).flatMap { transmissionDataList =>
//        val storeInMemoizedCacheResult = storeInMemoizedCache(transmissionDataList)
          for {
          subscribersParams<- getSubscribersAndFilteringParams()
          clientTransmissionRecords = generateClientTransmissionRecords(subscribersParams, transmissionDataList)
          results <- Future.sequence(clientTransmissionRecords.map { case (client, recordsForTransmission) => Future.sequence(recordsForTransmission
            .map(record => redisWsClientCache.storeVehicleActivityForClient(client, record)))
          }).map(_.flatten)
//          _ <- storeInMemoizedCacheResult
        } yield ()
      }
  }
  }

  private def generateClientTransmissionRecords(subscribersParams: Seq[(ClientId, FilteringParams)], transmissionDataList: Seq[BusPositionDataForTransmission]):  Seq[(ClientId, Seq[BusPositionDataForTransmission])]  = {
    subscribersParams.map {
      case (client, params) => client -> transmissionDataList.filter(_.satisfiesFilteringParams(params))
    }
  }

//  private def storeInMemoizedCache(transmissionData: Seq[BusPositionDataForTransmission]): Future[Seq[Unit]] = {
//    Future.sequence(transmissionData.map(redisWsClientCache.memoizeReadVehicleData))
//  }

  private def createDataForTransmission(stopArrivalRecord: StopArrivalRecord, timestamp: Long): Future[BusPositionDataForTransmission] = {

    val stopList = definitions.routeDefinitions.getOrElse(stopArrivalRecord.busRoute,
      throw new RuntimeException(s"Unable to locate ${stopArrivalRecord.busRoute} in definitions file"))

    val thisStop = stopList.find { case (index, _, _) => index == stopArrivalRecord.stopIndex }
      .getOrElse(throw new RuntimeException(s"Unable to locate index ${stopArrivalRecord.stopIndex} in definitions file for route ${stopArrivalRecord.busRoute}"))

    val isPenultimateStop = stopList.size == thisStop._1 + 2

    val nextStopOpt = stopList.find { case (index, _, _) => index == stopArrivalRecord.stopIndex + 1 }

    getArrivalTimeForNextStop(stopArrivalRecord).map(nextArrivalTimeOpt => {
      BusPositionDataForTransmission(
        stopArrivalRecord.vehicleId,
        stopArrivalRecord.busRoute,
        thisStop._2,
        timestamp,
        isPenultimateStop,
        nextStopOpt.map(_._2.stopName),
        nextArrivalTimeOpt,
        thisStop._3.map(_.toMovementInstructions))
    })
  }

  private def getArrivalTimeForNextStop(stopArrivalRecord: StopArrivalRecord): Future[Option[Long]] = {
    redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).map(arrivalTimes => {
      arrivalTimes.find(_.stopIndex == stopArrivalRecord.stopIndex + 1).map(_.arrivalTime)
    })
  }

  private def getSubscribersAndFilteringParams(): Future[Seq[(ClientId, FilteringParams)]] = {
    for {
      subscribers <- redisSubscriberCache.getListOfSubscribers
      filteringParams <- Future.sequence(subscribers.map(subscriber =>
        redisSubscriberCache.getParamsForSubscriber(subscriber)
          .map(_.map(fp => (subscriber, fp))))).map(_.flatten)

    } yield filteringParams
  }
}