package lbt.db

import com.typesafe.scalalogging.StrictLogging
import lbt.RedisDBConfig
import lbt.models.BusRoute

class RedisClient(dBConfig: RedisDBConfig) extends StrictLogging {

  private val client = new com.redis.RedisClient(host = dBConfig.host, port = dBConfig.port)
  logger.info(s"Using dbIndex ${dBConfig.dbName} for Redis Client")
  client.select(dBConfig.dbName.toInt)

  def persistStopToStopTime(route: BusRoute, fromStopSeq: Int, toStopSeq: Int, startTime: Long, duration: Int) = {
    //TODO do something with start time?
    val key = getKey(route, fromStopSeq, toStopSeq)
    client.lpush(key, duration)
    client.ltrim(key, 0, 99) //TODO max list length to be set in config
  }

  def getStopToStopTimes(route: BusRoute, fromStopSeq: Int, toStopSeq: Int) = {
    val key = getKey(route, fromStopSeq, toStopSeq)
    client.lrange(key, 0, 99).map(_.flatten.map(_.toInt))
  }

  private def getKey(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): String = {
    s"${route.id}_${route.direction}_${fromStopSeq.toString}_${toStopSeq.toString}"
  }

  def flushDB = client.flushdb

}