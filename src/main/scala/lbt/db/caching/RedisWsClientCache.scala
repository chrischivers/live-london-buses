package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import lbt.RedisConfig
import lbt.models.BusRoute
import redis.ByteStringFormatter

import scala.concurrent.{ExecutionContext, Future}

class RedisWsClientCache(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  def persistVehicleActivity(clientUUID: String, vehicleID: String, timeStamp: Long): Future[Unit] = {

    for {
      existsAlready <- client.exists(clientUUID)
      // The above updates the expiry only on first persistence. Going forward the expiry is updated when requests made.
      _ <- client.select(redisConfig.dbIndex)
      _ <- client.zadd(clientUUID, (timeStamp, vehicleID))
      _ <- if (!existsAlready) client.pexpire(clientUUID, redisConfig.wsClientCacheTTL.toMillis) else Future.successful(())
    } yield ()
  }

  def getVehicleActivityFor(clientUUID: String): Future[Seq[String]] = {
    for {
      _ <- client.select(redisConfig.dbIndex)
      activity <- client.zrange[String](clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.zremrangebyrank(clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.pexpire(clientUUID, redisConfig.wsClientCacheTTL.toMillis)
    } yield activity
  }

}
