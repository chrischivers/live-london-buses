package lbt.db

import akka.actor.ActorSystem
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import lbt.RedisConfig
import lbt.models.BusRoute
import redis.ByteStringFormatter

import scala.concurrent.{ExecutionContext, Future}

class RedisClient(redisConfig: RedisConfig)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem) extends StrictLogging {


  implicit val byteStringIntFormatter = new ByteStringFormatter[Int] {
    def serialize(integer: Int): ByteString = ByteString(integer.toString)
    def deserialize(bs: ByteString): Int = bs.utf8String.toInt
  }

  private val client = redis.RedisClient(host = redisConfig.host, port = redisConfig.port)
  logger.info(s"Using dbIndex ${redisConfig.dbIndex} for Redis Client")

  def persistStopToStopTime(route: BusRoute, fromStopSeq: Int, toStopSeq: Int, startTime: Long, duration: Int): Future[Boolean] = {
    //TODO do something with start time?
    val key = getKey(route, fromStopSeq, toStopSeq)
    for {
      _ <- client.select(redisConfig.dbIndex)
      _ <- client.lpush[Int](key, duration)
      trimRes <- client.ltrim(key, 0, redisConfig.maxListLength - 1)
    } yield trimRes
  }

  def getStopToStopTimes(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): Future[Seq[Int]] = {
    val key = getKey(route, fromStopSeq, toStopSeq)
    for {
      _ <- client.select(redisConfig.dbIndex)
      res <- client.lrange[Int](key, 0, redisConfig.maxListLength - 1)
    } yield res
  }

  private def getKey(route: BusRoute, fromStopSeq: Int, toStopSeq: Int): String = {
    s"${route.id}_${route.direction}_${fromStopSeq.toString}_${toStopSeq.toString}"
  }

  def flushDB: Future[Boolean] = for {
    _ <- client.select(redisConfig.dbIndex)
    res <- client.flushdb
  } yield res
}
