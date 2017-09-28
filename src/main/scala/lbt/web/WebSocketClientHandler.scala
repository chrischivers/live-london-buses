package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.BusRoute

import scala.concurrent.{ExecutionContext, Future}

class WebSocketClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def subscribe(clientUuid: String) = {
    logger.info(s"Subscribing client $clientUuid")
    redisSubscriberCache.subscribe(clientUuid, None)
  }

  def retrieveTransmissionDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityFor(uuid)
  }

  def queueTransmissionDataToClients(busPositionDataForTransmission: BusPositionDataForTransmission) = {
    //todo logic here to determine latlngbounds filtering parameters.
    for {
      subscribers <- redisSubscriberCache.getListOfSubscribers
      subscribersToBusRoute <- filterSubscribersForRoute(subscribers, busPositionDataForTransmission.busRoute)
      _ <- Future.sequence(subscribersToBusRoute.map(uuid => redisWsClientCache.storeVehicleActivity(uuid, busPositionDataForTransmission)))
    } yield ()
  }

  def updateFilteringParamsForClient(clientUuid: String, filteringParams: FilteringParams): Future[Unit] = {
      redisSubscriberCache.updateFilteringParameters(clientUuid, filteringParams)
  }

  private def filterSubscribersForRoute(subscribers: Seq[String], busRoute: BusRoute): Future[Seq[String]] = {
    Future.sequence(subscribers
      .map(subscriber => redisSubscriberCache.getParamsForSubscriber(subscriber)
        .map(_.flatMap(
          filteringParams => if (filteringParams.busRoutes.contains(busRoute))
            Some(subscriber) else None))))
      .map(_.flatten)
  }
}
