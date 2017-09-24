package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}

import scala.concurrent.Future

class WebSocketClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache) extends StrictLogging {

  def subscribe(uuid: String) = {
    logger.info(s"Subscribing client $uuid")
    redisSubscriberCache.subscribe(uuid, None)
  }

  def getDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityFor(uuid)
  }

  def persistData(busPositionDataForTransmission: BusPositionDataForTransmission) = {
    //todo logic here to determine which clients to push to
    redisSubscriberCache.getListOfSubscribers
  }

  def updateFilteringParamsForClient(uuid: String, filteringParams: FilteringParams): Future[Boolean] = {
      redisSubscriberCache.updateFilteringParameters(uuid, filteringParams)
  }
}
