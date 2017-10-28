package lbt.streaming

import akka.actor.{ActorRef, ActorSystem, Cancellable, Props}
import lbt.StreamingConfig
import lbt.common.Definitions
import lbt.db.caching.{RedisArrivalTimeLog, RedisSubscriberCache, RedisVehicleArrivalTimeLog, RedisWsClientCache}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CacheReaderScheduler(redisArrivalTimeCache: RedisArrivalTimeLog,
                           redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog,
                           redisSubscriberCache: RedisSubscriberCache,
                           redisWsClientCache: RedisWsClientCache,
                           definitions: Definitions,
                           streamingConfig: StreamingConfig)
                          (implicit actorSystem: ActorSystem, executionContext: ExecutionContext) {

  private val arrivalTimeCacheReader: ActorRef = actorSystem.actorOf(Props(
    new CacheReader(redisArrivalTimeCache, redisVehicleArrivalTimeLog, redisSubscriberCache, redisWsClientCache, definitions, streamingConfig)))

  private val scheduler: Cancellable =
    actorSystem.scheduler.schedule(
      0.seconds,
      streamingConfig.cachePollingInterval,
      arrivalTimeCacheReader,
      CacheReadCommand(streamingConfig.readAheadInCache.toMillis))

  def cancelScheduler = scheduler.cancel()
}

//TODO tests for this