package lbt.web

import java.util.UUID

import akka.actor.ActorSystem
import lbt.db.caching.{RedisArrivalTimeLog, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{BusStop, LatLng}
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MapsClientHandlerTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1 minute),
    interval = scaled(500 millis)
  )

  case class FixtureParam(webSocketClientHandler: MapsClientHandler, redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {
    implicit val actorSystem = ActorSystem()
    val redisConfig = config.redisConfig.copy(dbIndex = 1)

    val redisSubscriberCache = new RedisSubscriberCache(redisConfig)
    val redisWsClientCache = new RedisWsClientCache(redisConfig, redisSubscriberCache)
    val redisArrivalTimeLog = new RedisArrivalTimeLog(redisConfig)
    val webSocketClientHandler = new MapsClientHandler(redisSubscriberCache, redisWsClientCache, redisArrivalTimeLog)
    val testFixture = FixtureParam(webSocketClientHandler, redisSubscriberCache, redisWsClientCache)

    try {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    } finally {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }


  test("Web socket handler subscribes new client and checks they are already subscribed") { f =>

    val uuid = UUID.randomUUID().toString
    f.webSocketClientHandler.subscribe(uuid).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 1
    f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe Seq(uuid)
    f.webSocketClientHandler.isAlreadySubscribed(uuid).futureValue should be
    true
  }

  test("Web socket handler does not subscribe same client more than once") { f =>

    val uuid = UUID.randomUUID().toString
    f.webSocketClientHandler.subscribe(uuid).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 1
    f.webSocketClientHandler.subscribe(uuid).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 1
  }

  test("Transmission data for a client is retrieved") { f =>

    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData()
    val filteringParams = createFilteringParams()
    f.webSocketClientHandler.subscribe(uuid)
    f.redisSubscriberCache.updateFilteringParameters(uuid, filteringParams).futureValue
    f.redisWsClientCache.storeVehicleActivityForClient(uuid, busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.webSocketClientHandler.retrieveTransmissionDataForClient(uuid).futureValue).value
    result should have size 1
    result.head shouldBe busPositionData
  }

  test("Filtering params are updated for a subscribed client") { f =>

    val uuid = UUID.randomUUID().toString
    val filteringParams = createFilteringParams()
    f.webSocketClientHandler.subscribe(uuid).futureValue
    f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue should not be defined
    f.redisSubscriberCache.updateFilteringParameters(uuid, filteringParams).futureValue
    f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue.value shouldBe filteringParams
  }
}
