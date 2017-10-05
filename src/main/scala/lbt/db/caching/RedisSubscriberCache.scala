package lbt.db.caching

import akka.actor.ActorSystem
import cats.data.OptionT
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.RedisConfig
import lbt.models.{BusRoute, LatLngBounds}
import lbt.web.FilteringParams
import redis.api.Limit
import io.circe.generic.auto._
import io.circe.parser.{parse, _}
import io.circe.syntax._
import cats.implicits._

import scala.concurrent.{ExecutionContext, Future}

class RedisSubscriberCache(val redisConfig: RedisConfig)(implicit val executionContext: ExecutionContext, val actorSystem: ActorSystem) extends RedisClient {

  val subscribersKey = "SUBSCRIBERS"

  def subscribe(clientUUID: String, params: Option[FilteringParams]): Future[Unit] = {
    for {
      _ <- client.zadd(subscribersKey, (System.currentTimeMillis(), clientUUID))
      _ <- params.fold(Future.successful(()))( p => updateFilteringParameters(clientUUID, p))
    } yield ()
  }

  def subscriberExists(uuid: String) = {
    client.zscore(subscribersKey, uuid).map(_.isDefined)
  }

  def getListOfSubscribers: Future[Seq[String]] = {
    client.zrange[String](subscribersKey, 0, Long.MaxValue)
  }

  def getParamsForSubscriber(uuid: String): Future[Option[FilteringParams]] = {
    val paramsKey = getParamsKey(uuid)
    client.hmget[String](paramsKey, "busRoutes", "latLngBounds").map(params => parseFilteringParamsJson(params))
  }

  def updateFilteringParameters(uuid: String, filteringParams: FilteringParams): Future[Unit] = {
    val paramsKey = getParamsKey(uuid)
    for {
      existing <- subscriberExists(uuid)
      _ <- if (existing) client.hmset(paramsKey, filteringParamsToJsonMap(filteringParams)) else Future.successful()
      _ <- if (existing) updateSubscriberAliveTime(uuid) else Future.successful()
    } yield ()
  }

  def updateSubscriberAliveTime(uuid: String) = {
    val setClientParamsTTL = client.pexpire(getParamsKey(uuid), redisConfig.clientInactiveTime.toMillis)
    val updateSubscribersSetScore = client.zadd(subscribersKey, (System.currentTimeMillis(), uuid))
    val updateClientDataTTL = client.pexpire(uuid, redisConfig.clientInactiveTime.toMillis)
    for {
      _ <- setClientParamsTTL
      _ <- updateSubscribersSetScore
      _ <- updateClientDataTTL
    } yield ()
  }

  def cleanUpInactiveSubscribers = {
   client.zremrangebyscore(subscribersKey, Limit(0), Limit(System.currentTimeMillis() - redisConfig.clientInactiveTime.toMillis))
  }


  private def getParamsKey(uuid: String) = s"params-$uuid"

  private def filteringParamsToJsonMap(filteringParams: FilteringParams): Map[String, String] = {
    Map("busRoutes" -> filteringParams.busRoutes.asJson.noSpaces,
      "latLngBounds" -> filteringParams.latLngBounds.asJson.noSpaces)
  }

  private def parseFilteringParamsJson(input: Seq[Option[String]]): Option[FilteringParams] = {
    val flatList = input.flatten
    if (flatList.size != 2) None
    else for {
      busRoutes <- parse(flatList.head).flatMap(x => x.as[List[BusRoute]]).toOption
      latLngBounds <- parse(flatList(1)).flatMap(x => x.as[LatLngBounds]).toOption
    } yield {
      FilteringParams(busRoutes, latLngBounds)
    }
  }
}
