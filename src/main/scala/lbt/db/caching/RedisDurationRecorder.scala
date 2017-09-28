package lbt.db.caching

import akka.actor.ActorSystem
import akka.util.ByteString
import lbt.RedisConfig
import lbt.models.BusRoute
import redis.ByteStringFormatter

import scala.concurrent.{ExecutionContext, Future}

class RedisDurationRecorder(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  implicit val byteStringIntFormatter = new ByteStringFormatter[Int] {
    def serialize(integer: Int): ByteString = ByteString(integer.toString)
    def deserialize(bs: ByteString): Int = bs.utf8String.toInt
  }

  def persistStopToStopTime(route: BusRoute, fromStopSeq: Int, toStopSeq: Int, startTime: Long, duration: Int): Future[Unit] = {
    //TODO do something with start time?
    val key = generateKey(route, fromStopSeq, toStopSeq)
    for {
      _ <- client.lpush[Int](key, duration)
      _ <- client.ltrim(key, 0, redisConfig.durationMaxListLength - 1)
      _ <- client.pexpire(key, redisConfig.durationRecordTTL.toMillis)
    } yield ()
  }

  def getStopToStopTimes(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): Future[Seq[Int]] = {
    val key = generateKey(route, fromStopSeq, toStopSeq)

    client.lrange[Int](key, 0, redisConfig.durationMaxListLength - 1)
  }

  def getStopToStopAverageTime(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): Future[Double] = {
    getStopToStopTimes(route, fromStopSeq, toStopSeq).map(list=> calculateAverageTimes(list))
  }

  private def generateKey(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): String = {
    s"${route.id}_${route.direction}_${fromStopSeq.toString}_${toStopSeq.toString}"
  }

  private def calculateAverageTimes(timeDiffs: Seq[Int]): Double = {
    if (timeDiffs.isEmpty) 0.0
    else timeDiffs.sum.toDouble / timeDiffs.size.toDouble
  }

}
