package lbt.db.caching

import akka.actor.ActorSystem
import cats.implicits._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.{BusRoute, BusStop, LatLng, MovementInstruction}

import scala.concurrent.{ExecutionContext, Future}

case class BusPositionDataForTransmission(vehicleId: String, busRoute: BusRoute, startingBusStop: BusStop, startingTimeStamp: Long, isPenultimateStop: Boolean, nextStopName: Option[String], nextStopArrivalTime: Option[Long], movementInstructionsToNext: Option[List[MovementInstruction]])

class RedisWsClientCache(val redisConfig: RedisConfig, redisSubscriberCache: RedisSubscriberCache)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  def storeVehicleActivity(clientUUID: String, busPositionData: BusPositionDataForTransmission): Future[Unit] = {
    val jsonToStore = busPositionData.asJson.noSpaces
    for {
      existsAlready <- client.exists(clientUUID)
      _ <- client.zadd(clientUUID, (busPositionData.startingTimeStamp, jsonToStore))
      _ <- if (!existsAlready) client.pexpire(clientUUID, redisConfig.clientInactiveTime.toMillis) else Future.successful(())
        // The above updates the ttl only on first persistence. Going forward the expiry is updated when requests are made.
    } yield ()
  }

  def getVehicleActivityJsonFor(clientUUID: String): Future[String] = {

    val updateClientAliveTime = redisSubscriberCache.updateSubscriberAliveTime(clientUUID)
      for {
      results <- client.zrange[String](clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.zremrangebyrank(clientUUID, 0, results.size - 1)
      _ <- updateClientAliveTime
    } yield {
      val (parsingFailures, json) = results.map(res => parse(res)).toList.separate
      parsingFailures.foldLeft()((_, pf) => logger.error(s"Error parsing Bus Position Transmission Data json. $pf"))
      json.asJson.noSpaces
    }
  }
}
