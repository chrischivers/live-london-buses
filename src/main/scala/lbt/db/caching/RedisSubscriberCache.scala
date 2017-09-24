package lbt.db.caching

import akka.actor.ActorSystem
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.BusRoute
import redis.api.Limit

import scala.concurrent.{ExecutionContext, Future}

class RedisSubscriberCache(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  val subscribersKey = "SUBSCRIBERS"

  def subscribe(clientUUID: String, params: String): Future[Unit] = {
    val paramsKey = getParamsKey(clientUUID)
    for {
      _ <- client.select(redisConfig.dbIndex)
      _ <- client.zadd(subscribersKey, (System.currentTimeMillis(), clientUUID))
      _ <- client.set(paramsKey, params) //todo include filtering logic as parameters
      _ <- client.pexpire(paramsKey, redisConfig.clientInactiveTime.toMillis)
    } yield ()
  }

  def getListOfSubscribers: Future[Seq[String]] = {
    client.zrange[String](subscribersKey, 0, Long.MaxValue)
  }

  def getParamsForSubscriber(uuid: String): Future[Option[String]] = {
    val paramsKey = getParamsKey(uuid)
    for {
      _ <- client.pexpire(paramsKey, redisConfig.clientInactiveTime.toMillis)
      params <- client.get[String](paramsKey)
    } yield params
  }

  def updateSubscriberAliveTime(uuid: String) = {
    println("Updating subscriber alive time")
    val setTTL = client.pexpire(getParamsKey(uuid), redisConfig.clientInactiveTime.toMillis)
    val updateSubscribersSet = client.zadd(subscribersKey, (System.currentTimeMillis(), uuid))
    for {
      _ <- setTTL
      _ <- updateSubscribersSet
    } yield ()
  }

  def cleanUpInactiveSubscribers = {
   client.zremrangebyscore(subscribersKey, Limit(0), Limit(System.currentTimeMillis() - redisConfig.clientInactiveTime.toMillis))
  }

  private def getParamsKey(uuid: String) = s"params-$uuid"
}
