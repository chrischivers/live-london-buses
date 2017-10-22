package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import lbt.{RedisConfig, StreamingConfig}
import lbt.metrics.MetricsLogging
import lbt.models.BusRoute
import lbt.streaming.StopArrivalRecord
import redis.ByteStringFormatter
import redis.api.Limit

import scala.concurrent.{ExecutionContext, Future}

case class StopIndexArrivalTime(stopIndex: Int, arrivalTime: Long)

class RedisVehicleArrivalTimeLog(val redisConfig: RedisConfig, streamingConfig: StreamingConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  implicit private val intFormatter = new ByteStringFormatter[Int] {
    def serialize(int: Int): ByteString = ByteString(int.toString)
    def deserialize(bs: ByteString): Int = bs.utf8String.toInt
  }

  def addVehicleArrivalTime(stopArrivalRecord: StopArrivalRecord, arrivalTime: Long): Future[Unit] = {
    val key = generateVehicleRouteKey(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute)
    for {
      _ <- client.zadd(key, (arrivalTime, stopArrivalRecord.stopIndex))
      _ <- client.expire(key, streamingConfig.idleTimeBeforeVehicleDeleted.toSeconds)
    } yield ()
  }

  def getArrivalTimes(vehicleId: String, busRoute: BusRoute): Future[Seq[StopIndexArrivalTime]] = {
    val key = generateVehicleRouteKey(vehicleId, busRoute)
    for {
      arrivalRecords <- client.zrangeWithscores[Int](key, 0, Long.MaxValue)
    } yield arrivalRecords.map{case(index, time) => StopIndexArrivalTime(index, time.toLong)}.sortBy(_.stopIndex)
  }

  private def generateVehicleRouteKey(vehicleId: String, busRoute: BusRoute): String = {
    s"$vehicleId-${busRoute.id}-${busRoute.direction}"
  }
}
