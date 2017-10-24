package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.metrics.MetricsLogging
import lbt.streaming.StopArrivalRecord
import redis.ByteStringFormatter
import redis.api.Limit

import scala.concurrent.{ExecutionContext, Future}

class RedisArrivalTimeLog(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  private val ARRIVAL_TIMES_KEY = "ARRIVAL_TIMES"

  implicit private val stopArrivalRecordFormatter = new ByteStringFormatter[StopArrivalRecord] {
    def serialize(arrivalTimeRecord: StopArrivalRecord): ByteString = ByteString(arrivalTimeRecord.asJson.noSpaces)

    def deserialize(bs: ByteString): StopArrivalRecord = parse(bs.utf8String).fold(failure =>
      throw new RuntimeException(s"error parsing json from redis arrival cache. Failure: $failure"),
      json => json.as[StopArrivalRecord].fold(failure =>
        throw new RuntimeException(s"error decoding json from redis arrival cache. Failure: $failure"), identity))
  }

  def addArrivalRecord(arrivalTime: Long, arrivalTimeRecord: StopArrivalRecord): Future[Unit] = {
    client.zadd(ARRIVAL_TIMES_KEY, (arrivalTime, arrivalTimeRecord)).map(_=> ())
  }

  def takeArrivalRecordsUpTo(arrivalTimesUpTo: Long): Future[Seq[(StopArrivalRecord, Long)]] = {
    for {
      arrivalRecords <- client.zrangebyscoreWithscores[StopArrivalRecord](ARRIVAL_TIMES_KEY, Limit(0), Limit(arrivalTimesUpTo))
      _ <- client.zremrangebyscore(ARRIVAL_TIMES_KEY, Limit(0), Limit(arrivalTimesUpTo))
    } yield arrivalRecords.map(x => (x._1, x._2.toLong))
  }
}
