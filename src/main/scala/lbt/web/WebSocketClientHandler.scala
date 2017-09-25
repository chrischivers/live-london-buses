package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.BusRoute

import scala.concurrent.{ExecutionContext, Future}

class WebSocketClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def subscribe(uuid: String) = {
    logger.info(s"Subscribing client $uuid")
    redisSubscriberCache.subscribe(uuid, None)
  }

  def getDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityFor(uuid)
  }

  def persistData(busPositionDataForTransmission: BusPositionDataForTransmission) = {
    //todo logic here to determine which clients to push to. Currently pushing to all.
    for {
      subscribers <- redisSubscriberCache.getListOfSubscribers
      subscribersToBusRoute <- filterSubscribersForRoute(subscribers, busPositionDataForTransmission.busRoute)
      _ <- Future.sequence(subscribersToBusRoute.map(uuid => redisWsClientCache.storeVehicleActivity(uuid, busPositionDataForTransmission)))
    } yield ()
  }

  def updateFilteringParamsForClient(uuid: String, filteringParams: FilteringParams): Future[Unit] = {
      redisSubscriberCache.updateFilteringParameters(uuid, filteringParams)
  }

  def filterSubscribersForRoute(subscribers: Seq[String], busRoute: BusRoute): Future[Seq[String]] = {
    Future.sequence(subscribers
      .map(subscriber => redisSubscriberCache.getParamsForSubscriber(subscriber)
        .map(_.flatMap(
          filteringParams => if (filteringParams.busRoutes.contains(busRoute))
            Some(subscriber) else None))))
      .map(_.flatten)
  }
}
