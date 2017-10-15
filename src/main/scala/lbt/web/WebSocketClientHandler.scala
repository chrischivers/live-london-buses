package lbt.web

import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.parser.parse
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{BusRoute, LatLng}

import scala.concurrent.{ExecutionContext, Future}

class WebSocketClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def subscribe(clientUuid: String) = {
    logger.info(s"Subscribing client $clientUuid")
    redisSubscriberCache.subscribe(clientUuid, None)
  }

  def isAlreadySubscribed(uuid: String): Future[Boolean] = {
    logger.debug(s"Checking if $uuid already exists")
    redisSubscriberCache.subscriberExists(uuid)
  }

  def retrieveTransmissionDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityJsonForClient(uuid)
  }

  def updateFilteringParamsForClient(clientUuid: String, filteringParams: FilteringParams): Future[Unit] = {
    redisSubscriberCache.updateFilteringParameters(clientUuid, filteringParams)
  }

//  def addInProgressDataToClientCache(clientUUID: String, filteringParams: FilteringParams) = for {
//    inProgressData <- getInProgressData(filteringParams)
//    _ <- Future.sequence(inProgressData.map(rec => redisWsClientCache.storeVehicleActivityForClient(clientUUID, rec)))
//  } yield ()
//
//  private def getInProgressData(filteringParams: FilteringParams): Future[Seq[BusPositionDataForTransmission]] = {
//    val now = System.currentTimeMillis()
//
//    redisWsClientCache.getRecordsInMemoizeCache().map { y =>
//      y.flatMap(parseWebsocketCacheResult)
//        .filter(_.satisfiesFilteringParams(filteringParams))
//        .filter(rec => now > rec.startingTimeStamp &&
//          rec.nextStopArrivalTime.fold(true)(nextStop => nextStop > now))
//        .groupBy(_.vehicleId).flatMap { case (_, records) =>
//        records.sortBy(_.nextStopArrivalTime.getOrElse(0L)).reverse.headOption
//      }.toList
//    }
//  }

  private def parseWebsocketCacheResult(str: String): Option[BusPositionDataForTransmission] = {
    parse(str).flatMap(_.as[BusPositionDataForTransmission]).toOption
  }
}
