package lbt.db.caching

import akka.actor.ActorSystem
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.BusRoute

import scala.concurrent.{ExecutionContext, Future}

class RedisSubscriberCache(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  val subscribersKey = "SUBSCRIBERS"

  def addNewSubscriber(clientUUID: String): Future[Unit] = {
    for {
      _ <- client.select(redisConfig.dbIndex)
      _ <- client.sadd(subscribersKey, clientUUID)
    } yield ()
  }

  def getListOfSubscribers: Future[Seq[String]] = {
    for {
      _ <- client.select(redisConfig.dbIndex)
      subscribers <- client.smembers[String](subscribersKey)
    } yield subscribers
  }



}
