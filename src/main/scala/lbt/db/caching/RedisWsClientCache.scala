package lbt.db.caching

import akka.actor.ActorSystem
import cats.Alternative
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.BusRoute
import io.circe._
import cats.implicits._
import io.circe.generic.semiauto._
import io.circe.parser._
import cats.Alternative._

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
    // The above updates the ttl only on first persistence. Going forward the expiry is updated when requests are made.
    } yield ()
  }

  def getVehicleActivityFor(clientUUID: String): Future[String] = {
    for {
      _ <- client.select(redisConfig.dbIndex)
      results <- client.zrange[String](clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.zremrangebyrank(clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.pexpire(clientUUID, redisConfig.wsClientCacheTTL.toMillis)
    } yield {
      val (parsingFailures, json) = results.map(res => parse(res)).toList.separate
      //todo do something with failures
      json.asJson.noSpaces
    }
  }

}
