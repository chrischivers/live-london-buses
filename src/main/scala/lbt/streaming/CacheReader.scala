package lbt.streaming

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.StreamingConfig
import lbt.common.Definitions
import lbt.db.caching._
import lbt.metrics.MetricsLogging
import lbt.web.FilteringParams

import scala.concurrent.{ExecutionContext, Future}
import scalacache.ScalaCache
import scalacache.guava.GuavaCache
import scalacache._
import scala.concurrent.duration._


case class CacheReadCommand(readTimeAhead: Long)

class CacheReader(redisArrivalTimeLog: RedisArrivalTimeLog, redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog, redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache, definitions: Definitions, streamingConfig: StreamingConfig)(implicit executionContext: ExecutionContext) extends Actor with StrictLogging {

  type ClientId = String
  implicit private val outgoingRecordsCache: ScalaCache[NoSerialization] = ScalaCache(GuavaCache())

  override def receive = {
    case CacheReadCommand(time) =>
      MetricsLogging.measureCacheReadProcess {
        transferArrivalTimesToWebSocketCache(time)
      }
    case unknown => throw new RuntimeException(s"Unknown command received by arrival time cache reader: $unknown")
  }

  def transferArrivalTimesToWebSocketCache(arrivalTimesUpTo: Long) = for {
      allRecords <- redisArrivalTimeLog.takeArrivalRecordsUpTo(System.currentTimeMillis() + arrivalTimesUpTo)
      _ = MetricsLogging.incrCachedRecordsProcessed(allRecords.size)
      notLastStopRecords = filterOutLastStops(allRecords)
      notAlreadySent <- filterOutRecordsAlreadySent(notLastStopRecords)
      transmissionDataList <- getTransmissionList(notAlreadySent)
      subscribersParams <- getSubscribersAndFilteringParams()
      clientTransmissionRecords = generateClientTransmissionRecords(subscribersParams, transmissionDataList)
      _ <- storeInClientCaches(clientTransmissionRecords)
      _ <- addToInProgressCache(transmissionDataList)
    } yield ()

  private def getTransmissionList(filteredRecords: Seq[(StopArrivalRecord, Long)]): Future[Seq[BusPositionDataForTransmission]] = {
    Future.sequence(filteredRecords
      .map { case (stopArrivalRecord, timestamp) => for {
        transmissionDataList <- createDataForTransmission(stopArrivalRecord, timestamp)
        _ <- addToArrivalRecordsHandledCache(stopArrivalRecord)
      } yield transmissionDataList
      })
  }

  private def storeInClientCaches(clientTransmissionRecords: Seq[(ClientId, Seq[BusPositionDataForTransmission])]): Future[Seq[Unit]] = {
    Future.sequence(clientTransmissionRecords.map { case (client, recordsForTransmission) => Future.sequence(recordsForTransmission
      .map(record => redisWsClientCache.storeVehicleActivityForClient(client, record)))
    }).map(_.flatten)
  }

  private def generateClientTransmissionRecords(subscribersParams: Seq[(ClientId, FilteringParams)], transmissionDataList: Seq[BusPositionDataForTransmission]): Seq[(ClientId, Seq[BusPositionDataForTransmission])] = {
    subscribersParams.map {
      case (client, params) => client -> transmissionDataList.filter(_.satisfiesFilteringParams(params))
    }
  }

  private def addToArrivalRecordsHandledCache(stopArrivalRecord: StopArrivalRecord): Future[Any] = {
    put(stopArrivalRecord)(true, ttl = Some(streamingConfig.outgoingCacheRetain))
  }

  private def hasAlreadyBeenSent(stopArrivalRecord: StopArrivalRecord): Future[Boolean] = {
    get[Boolean, NoSerialization](stopArrivalRecord).map(_.isDefined)
  }

  private def filterOutLastStops(records: Seq[(StopArrivalRecord, Long)]): Seq[(StopArrivalRecord, Long)] =
    records.filter(_._1.lastStop != true)

  private def filterOutRecordsAlreadySent(records: Seq[(StopArrivalRecord, Long)]) ={
    Future.sequence{
      records.map(recTime => hasAlreadyBeenSent(recTime._1).map(alreadySent => if(alreadySent) None else Some(recTime)))
    }.map(_.flatten)
  }

  private def addToInProgressCache(transmissionData: Seq[BusPositionDataForTransmission]): Future[Seq[Unit]] = {
    Future.sequence(transmissionData.map(redisWsClientCache.storeVehicleActivityInProgress))
  }

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
        timestamp,
        thisStop._2.latLng,
        isPenultimateStop,
        nextStopOpt.map(_._2.stopName),
        nextStopOpt.map(_._1),
        nextArrivalTimeOpt,
        thisStop._3.map(_.toMovementInstructions),
        stopArrivalRecord.destinationText)
    })
  }

  private def getArrivalTimeForNextStop(stopArrivalRecord: StopArrivalRecord): Future[Option[Long]] = {
    redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).map(arrivalTimes => {
      arrivalTimes.find(_.stopIndex == stopArrivalRecord.stopIndex + 1).map(_.arrivalTime)
    })
  }

  private def getParametersForSubscriber(subscriber: String) = {
      redisSubscriberCache.getParamsForSubscriber(subscriber)
        .map(_.map(fp => (subscriber, fp)))
  }

  private def getSubscribersAndFilteringParams(): Future[Seq[(ClientId, FilteringParams)]] = for {
      _ <- redisSubscriberCache.cleanUpInactiveSubscribers
      subscribers <- redisSubscriberCache.getListOfSubscribers
      _ = MetricsLogging.setUsersCurrentlySubscribed(subscribers.size)
      filteringParams <- Future.sequence(subscribers
        .map(subscriber => getParametersForSubscriber(subscriber)))
        .map(_.flatten)

    } yield filteringParams
}