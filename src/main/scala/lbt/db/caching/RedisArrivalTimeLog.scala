package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import lbt.RedisConfig
import lbt.metrics.MetricsLogging
import lbt.models.BusRoute
import redis.ByteStringFormatter
import redis.api.Limit
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import lbt.streaming.StopArrivalRecord

import scala.concurrent.{ExecutionContext, Future}

class RedisArrivalTimeLog(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  implicit private val stopArrivalRecordFormatter = new ByteStringFormatter[StopArrivalRecord] {
    def serialize(arrivalTimeRecord: StopArrivalRecord): ByteString = ByteString(arrivalTimeRecord.asJson.noSpaces)

    def deserialize(bs: ByteString): StopArrivalRecord = parse(bs.utf8String).fold(failure =>
      throw new RuntimeException(s"error parsing json from redis arrival cache. Failure: $failure"),
      json => json.as[StopArrivalRecord].fold(failure =>
        throw new RuntimeException(s"error decoding json from redis arrival cache. Failure: $failure"), identity))
  }

  private val ARRIVAL_TIMES_KEY = "ARRIVAL_TIMES"

  def addArrivalRecord(arrivalTime: Long, arrivalTimeRecord: StopArrivalRecord): Future[Unit] = {
    for {
      _ <- client.zadd(ARRIVAL_TIMES_KEY, (arrivalTime, arrivalTimeRecord))
      _ = MetricsLogging.incrArrivalTimesLogged
    } yield ()
  }

  def getArrivalRecords(arrivalTimesUpTo: Long): Future[Seq[(StopArrivalRecord, Double)]] = {
    for {
      arrivalRecords <- client.zrangebyscoreWithscores[StopArrivalRecord](ARRIVAL_TIMES_KEY, Limit(0), Limit(arrivalTimesUpTo))
      _ <- client.zremrangebyscore(ARRIVAL_TIMES_KEY, Limit(0), Limit(arrivalTimesUpTo))
    } yield arrivalRecords
  }
}
