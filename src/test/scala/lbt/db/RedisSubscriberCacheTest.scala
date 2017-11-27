package lbt.db

import java.util.UUID

import akka.actor.ActorSystem
import lbt.{ConfigLoader, LBTConfig, SharedTestFeatures}
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{BusRoute, BusStop, LatLng, LatLngBounds}
import lbt.web.FilteringParams
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class RedisSubscriberCacheTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with EitherValues {

  val config: LBTConfig = ConfigLoader.defaultConfig

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second)
  )

  case class FixtureParam(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val modifiedConfig = config.redisConfig.copy(dbIndex = 1, clientInactiveTime = 10 seconds) // 1 = test, 0 = main
    val redisSubscriberCache = new RedisSubscriberCache(modifiedConfig)
    val redisWsClientCache = new RedisWsClientCache(modifiedConfig, redisSubscriberCache)
    val testFixture = FixtureParam(redisSubscriberCache, redisWsClientCache)

    try {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
    }
  }

  test("New subscribers are added to the cache and retrieved together") { f =>

    val uuid1 = UUID.randomUUID().toString
    val uuid2 = UUID.randomUUID().toString
    f.redisSubscriberCache.subscribe(uuid1, None).futureValue
    f.redisSubscriberCache.subscribe(uuid2, None).futureValue

    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1, uuid2)
  }

  test("A new subscriber can be unsubscribed") { f =>

    val uuid1 = UUID.randomUUID().toString
    f.redisSubscriberCache.subscribe(uuid1, None).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.unsubscribe(uuid1).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe empty
  }

  test("A new subscriber is added to the cache and their parameters are obtainable") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.subscribe(uuid1, Some(params1)).futureValue

    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
  }

  test("A new subscriber is added to the cache and parameters are updated") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.subscribe(uuid1, None).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue should not be defined

    f.redisSubscriberCache.updateFilteringParameters(uuid1, params1).futureValue

    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
  }

  test("Clean Up Subscribers removes subscribers from cache if no retrieval activity") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.subscribe(uuid1, Some(params1)).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
    f.redisWsClientCache.storeVehicleActivityForClient(uuid1, createBusPositionData()).futureValue
    Thread.sleep(11000)
    f.redisSubscriberCache.cleanUpInactiveSubscribers.futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 0
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue should not be defined
    f.redisWsClientCache.getVehicleActivityJsonForClient(uuid1).futureValue shouldBe "[]"
  }

  test("Clean Up Subscribers does not remove subscribers from cache if there has been retrieval activity") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.subscribe(uuid1, Some(params1)).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
    f.redisWsClientCache.storeVehicleActivityForClient(uuid1, createBusPositionData()).futureValue
    f.redisWsClientCache.getVehicleActivityJsonForClient(uuid1).futureValue
    Thread.sleep(6000)
    f.redisWsClientCache.getVehicleActivityJsonForClient(uuid1).futureValue
    Thread.sleep(6000)
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
  }

  test("Parameters are ignored if received before subscriber is in cache") { f =>
    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.updateFilteringParameters(uuid1, params1).futureValue
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue should not be defined
    f.redisSubscriberCache.getListOfSubscribers.futureValue should not contain uuid1
  }

  test("The same uuid is able to subscribe twice, with the second overwriting the first") { f =>

    val uuid = UUID.randomUUID().toString
    val params1 = createFilteringParams(busRoutes = Some(List(BusRoute("3", "outbound"))))
    val params2 = createFilteringParams(busRoutes = Some(List(BusRoute("3", "inbound"))))

    f.redisSubscriberCache.subscribe(uuid, Some(params1)).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 1
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid)
    f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue shouldBe Some(params1)

    f.redisSubscriberCache.subscribe(uuid, Some(params2)).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 1
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid)
    f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue shouldBe Some(params2)
  }

  test("New filtering parameters received replace those already there") { f =>
    val uuid1 = UUID.randomUUID().toString
    val params1 = createFilteringParams()
    f.redisSubscriberCache.subscribe(uuid1, Some(params1)).futureValue
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue.value shouldBe params1

    val params2 = createFilteringParams(busRoutes = Some(List(BusRoute("100", "outbound"))))
    f.redisSubscriberCache.updateFilteringParameters(uuid1, params2).futureValue
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue.value shouldBe params2

  }
}