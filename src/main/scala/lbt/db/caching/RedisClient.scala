package lbt.db.caching

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import lbt.RedisConfig

import scala.concurrent.{ExecutionContext, Future}

trait RedisClient extends StrictLogging {
  implicit val executionContext: ExecutionContext
  implicit val actorSystem: ActorSystem
  val redisConfig: RedisConfig
  lazy val client: redis.RedisClient = {
    val cl = redis.RedisClient(host = redisConfig.host, port = redisConfig.port)
    cl.select(redisConfig.dbIndex)
    cl
  }

  def flushDB: Future[Boolean] = for {
    _ <- client.select(redisConfig.dbIndex)
    res <- client.flushdb
  } yield res
}
