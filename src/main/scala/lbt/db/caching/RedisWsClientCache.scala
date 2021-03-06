package lbt.db.caching

import akka.actor.ActorSystem
import cats.implicits._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.{BusRoute, BusStop, LatLng, MovementInstruction}
import lbt.web.FilteringParams
import redis.api.Limit

import scala.concurrent.{ExecutionContext, Future}

case class BusPositionDataForTransmission(vehicleId: String,
                                          busRoute: BusRoute,
                                          startingTime: Long,
                                          startingLatLng: LatLng,
                                          deleteAfter: Boolean,
                                          nextStopName: Option[String],
                                          nextStopIndex: Option[Int],
                                          nextStopArrivalTime: Option[Long],
                                          movementInstructionsToNext: Option[List[MovementInstruction]],
                                          destination: String) {

  def satisfiesFilteringParams(fps: FilteringParams): Boolean = {
    fps.busRoutes.fold(true)(_.contains(this.busRoute)) &&
      (fps.latLngBounds.isWithinBounds(this.startingLatLng) ||
        this.movementInstructionsToNext.fold(false)( ins => fps.latLngBounds.isWithinBounds(ins.last.to)))
  }
}

class RedisWsClientCache(val redisConfig: RedisConfig, redisSubscriberCache: RedisSubscriberCache)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  val MEMOIZED_RECORDS_KEY = "MEMOIZED_RECORDS"

  def storeVehicleActivityForClient(clientUUID: String, busPositionData: BusPositionDataForTransmission): Future[Unit] = {
    val jsonToStore = busPositionData.asJson.noSpaces
    for {
      existsAlready <- client.exists(clientUUID)
      _ <- client.zadd(clientUUID, (busPositionData.startingTime, jsonToStore))
      _ <- if (!existsAlready) client.pexpire(clientUUID, redisConfig.clientInactiveTime.toMillis) else Future.successful(())
    // The above updates the ttl only on first persistence. Going forward the expiry is updated when requests are made.
    } yield ()
  }

  def getVehicleActivityJsonForClient(clientUUID: String): Future[String] = {

    val updateClientAliveTime = redisSubscriberCache.updateSubscriberAliveTime(clientUUID)
    for {
      results <- client.zrange[String](clientUUID, 0, redisConfig.wsClientCacheMaxResultsReturned - 1)
      _ <- client.zremrangebyrank(clientUUID, 0, results.size - 1)
      _ <- updateClientAliveTime
    } yield s"[${results.mkString(",")}]"
  }

  def storeVehicleActivityInProgress(busPositionData: BusPositionDataForTransmission): Future[Unit] = {
    val jsonToStore = busPositionData.asJson.noSpaces
    for {
      _ <- busPositionData.nextStopArrivalTime.fold(Future.successful())(nextArrival => client.zadd(MEMOIZED_RECORDS_KEY, (nextArrival, jsonToStore)).map(_ => ()))
      _ <- client.zremrangebyscore(MEMOIZED_RECORDS_KEY, Limit(0), Limit(System.currentTimeMillis()))
    } yield ()
  }

  def getVehicleActivityInProgress(): Future[Seq[String]] = {
    for {
      results <- client.zrange[String](MEMOIZED_RECORDS_KEY, 0, Long.MaxValue)
    } yield results
  }
}
