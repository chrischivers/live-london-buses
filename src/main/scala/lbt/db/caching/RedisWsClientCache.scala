package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import lbt.RedisConfig
import io.circe.generic.auto._
import lbt.models.BusRoute
import redis.ByteStringFormatter
import io.circe.generic.JsonCodec, io.circe.syntax._

import scala.concurrent.{ExecutionContext, Future}

case class BusPositionDataForTransmission(vehicleId: String, busRoute: BusRoute, lat: Double, lng: Double, nextStopName: String, timeStamp: Long)

class RedisWsClientCache(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {


  def storeVehicleActivity(clientUUID: String, busPositionData: BusPositionDataForTransmission): Future[Unit] = {
    val jsonToStore = busPositionData.asJson.noSpaces
    for {
      existsAlready <- client.exists(clientUUID)
      _ <- client.select(redisConfig.dbIndex)
      _ <- client.zadd(clientUUID, (busPositionData.timeStamp, jsonToStore))
      _ <- if (!existsAlready) client.pexpire(clientUUID, redisConfig.wsClientCacheTTL.toMillis) else Future.successful(())
    // The above updates the ttl only on first persistence. Going forward the expiry is updated when requests made.
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
