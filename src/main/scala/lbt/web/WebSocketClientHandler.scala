package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}

import scala.concurrent.Future

class WebSocketClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache) extends StrictLogging {

  def subscribe(uuid: String) = {
    logger.info(s"Subscribing client $uuid")
    redisSubscriberCache.addNewSubscriber(uuid)
  }

  def getDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityFor(uuid)
  }
}
