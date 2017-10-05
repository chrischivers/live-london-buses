package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{BusRoute, LatLng, LatLngBounds}

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
    redisWsClientCache.getVehicleActivityFor(uuid)
  }

  def queueTransmissionDataToClients(data: BusPositionDataForTransmission) = {
    //todo logic here to determine latlngbounds filtering parameters.
    for {
      subscribers <- redisSubscriberCache.getListOfSubscribers
      subscribersToBusRoute <- filterSubscribers(subscribers, data.busRoute, data.startingBusStop.latLng)
      _ <- Future.sequence(subscribersToBusRoute.map(uuid => redisWsClientCache.storeVehicleActivity(uuid, data)))
    } yield ()
  }

  def updateFilteringParamsForClient(clientUuid: String, filteringParams: FilteringParams): Future[Unit] = {
      redisSubscriberCache.updateFilteringParameters(clientUuid, filteringParams)
  }

  private def filterSubscribers(subscribers: Seq[String], busRoute: BusRoute, busStopLatLng: LatLng): Future[Seq[String]] = {
    Future.sequence(subscribers
      .map(subscriber => redisSubscriberCache.getParamsForSubscriber(subscriber)
        .map(_.flatMap(
          filteringParams => if (filteringParams.busRoutes.contains(busRoute) && filteringParams.latLngBounds.isWithinBounds(busStopLatLng))
            Some(subscriber) else None))))
      .map(_.flatten)
  }

}
