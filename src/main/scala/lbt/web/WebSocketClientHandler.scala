package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}

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
}
