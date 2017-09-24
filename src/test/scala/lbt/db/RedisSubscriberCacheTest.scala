package lbt.db

import java.util.UUID

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.BusRoute
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class RedisSubscriberCacheTest extends fixture.FunSuite with ScalaFutures with OptionValues with EitherValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second)
  )

  case class FixtureParam(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val modifiedConfig = config.redisDBConfig.copy(dbIndex = 1, clientInactiveTime = 10 seconds) // 1 = test, 0 = main
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
    f.redisSubscriberCache.subscribe(uuid1, "params1").futureValue
    f.redisSubscriberCache.subscribe(uuid2, "params2").futureValue

    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1, uuid2)
  }

  test("A new subscriber is added to the cache and their parameters are obtainable") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = "params1"
    f.redisSubscriberCache.subscribe(uuid1,params1).futureValue

    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
  }

  test("Clean Up Subscribers removes subscribers from cache if no retrieval activity") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = "params1"
    f.redisSubscriberCache.subscribe(uuid1,params1).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
    f.redisWsClientCache.storeVehicleActivity(uuid1, createBusPositionData()).futureValue
    Thread.sleep(11000)
    f.redisSubscriberCache.cleanUpInactiveSubscribers.futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should have size 0
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue should not be defined
    f.redisWsClientCache.getVehicleActivityFor(uuid1).futureValue shouldBe "[]"
  }

  test("Clean Up Subscribers does not remove subscribers from cache if there has been retrieval activity") { f =>

    val uuid1 = UUID.randomUUID().toString
    val params1 = "params1"
    f.redisSubscriberCache.subscribe(uuid1, params1).futureValue
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
    f.redisWsClientCache.storeVehicleActivity(uuid1, createBusPositionData()).futureValue
    f.redisWsClientCache.getVehicleActivityFor(uuid1).futureValue
    Thread.sleep(6000)
    f.redisWsClientCache.getVehicleActivityFor(uuid1).futureValue
    Thread.sleep(6000)
    f.redisSubscriberCache.getListOfSubscribers.futureValue should contain theSameElementsAs List(uuid1)
    f.redisSubscriberCache.getParamsForSubscriber(uuid1).futureValue shouldBe Some(params1)
  }

  private def createBusPositionData(vehicleId: String = Random.nextString(10),
                                    busRoute: BusRoute = BusRoute("3", "outbound"),
                                    lat: Double = 51.4217,
                                    lng: Double = -0.077507,
                                    nextStopName: String = "NextStop",
                                    timeStamp: Long = System.currentTimeMillis()) = {
    BusPositionDataForTransmission(vehicleId, busRoute, lat, lng, nextStopName, timeStamp)
  }
}