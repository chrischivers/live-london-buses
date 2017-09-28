package lbt.db

import java.util.UUID

import akka.actor.ActorSystem
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import lbt.{ConfigLoader, LBTConfig}
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{BusRoute, BusStop}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class RedisWsClientCacheTest extends fixture.FunSuite with ScalaFutures with OptionValues with EitherValues {

  val config: LBTConfig = ConfigLoader.defaultConfig

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisWSClientCache: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val modifiedConfig = config.redisDBConfig.copy(dbIndex = 1, clientInactiveTime = 5 seconds) // 1 = test, 0 = main
    val redisSubscriberCache = new RedisSubscriberCache(modifiedConfig)
    val redisWSClientCache = new RedisWsClientCache(modifiedConfig, redisSubscriberCache)
    val testFixture = FixtureParam(redisWSClientCache)

    try {
      redisWSClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisWSClientCache.flushDB.futureValue
    }
  }

  test("Vehicle activity persisted to Redis sorted set and retrieved ordered by timestamp (oldest first)") { f =>

    val uuid = UUID.randomUUID().toString
    val busPosData1 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis())
    val busPosData2 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() + 60000)
    val busPosData3 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() - 60000)
    f.redisWSClientCache.storeVehicleActivity(uuid, busPosData1).futureValue
    f.redisWSClientCache.storeVehicleActivity(uuid, busPosData2).futureValue
    f.redisWSClientCache.storeVehicleActivity(uuid, busPosData3).futureValue

    val results = f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue
    val parsedResults: List[BusPositionDataForTransmission] = parseWebsocketCacheResult(results)
    parsedResults should have size 3
    parsedResults.head shouldBe busPosData3
    parsedResults(1)  shouldBe busPosData1
    parsedResults(2) shouldBe busPosData2
  }

  test("When vehicle activity retrieved from Redis, the records are purged") { f =>

    val uuid = UUID.randomUUID().toString
    val busPosData1 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() + 60000)
    val busPosData2 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis())
    f.redisWSClientCache.storeVehicleActivity(uuid, busPosData1).futureValue
    f.redisWSClientCache.storeVehicleActivity(uuid, busPosData2).futureValue

    val results = f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue
    val parsedResults: List[BusPositionDataForTransmission] = parseWebsocketCacheResult(results)
    parsedResults should have size 2
    parsedResults.head shouldBe busPosData2
    parsedResults(1) shouldBe busPosData1

    val resultsAgain = f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue
    parseWebsocketCacheResult(resultsAgain) should have size 0
  }

  test("When no vehicle activity in Redis for uuid, an empty result set is retrieved)") { f =>

    val uuid = UUID.randomUUID().toString

    val results = f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue
    parseWebsocketCacheResult(results) should have size 0
  }

  test("Results coming back from Redis for uuid are limited to 100 per request") { f =>

    val uuid = UUID.randomUUID().toString
    Future.sequence((0 to 100).map { _ =>
      f.redisWSClientCache.storeVehicleActivity(uuid, createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() + Random.nextInt(60000)))
    }).futureValue


    parseWebsocketCacheResult(f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue) should have size 100
    parseWebsocketCacheResult(f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue) should have size 1
    parseWebsocketCacheResult(f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue) should have size 0
  }


  test("Vehicle activity for a uuid in Redis expires if nothing persisted in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val busPos1 = createBusPositionData()
    val busPos2 = createBusPositionData()

    f.redisWSClientCache.storeVehicleActivity(uuid, busPos1).futureValue
    f.redisWSClientCache.storeVehicleActivity(uuid, busPos2).futureValue
    Thread.sleep(5500)
    parseWebsocketCacheResult(f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue) should have size 0
  }

  test("Vehicle activity for a uuid in Redis expires if no get requests received in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val busPos1 = createBusPositionData()
    val busPos2 = createBusPositionData()

    f.redisWSClientCache.storeVehicleActivity(uuid, busPos1).futureValue
    Thread.sleep(3000)
    f.redisWSClientCache.storeVehicleActivity(uuid, busPos2).futureValue
    Thread.sleep(3000)
    parseWebsocketCacheResult(f.redisWSClientCache.getVehicleActivityFor(uuid).futureValue) should have size 0
  }

  private def createBusPositionData(vehicleId: String = Random.nextString(10),
                                    busRoute: BusRoute = BusRoute("3", "outbound"),
                                    busStop: BusStop = BusStop("490003059E", "Abingdon Street", 51.49759, -0.125605),
                                    nextStopName: String = "NextStop",
                                    arrivalTimeStamp: Long = System.currentTimeMillis(),
                                    durationToNextStopOpt: Option[Int] = Some(100)) = {
    BusPositionDataForTransmission(vehicleId, busRoute, busStop, arrivalTimeStamp, nextStopName, durationToNextStopOpt)
  }

  private def parseWebsocketCacheResult(str: String): List[BusPositionDataForTransmission] = {
    implicit val busRouteDecoder: Decoder[BusRoute] = deriveDecoder[BusRoute]
    implicit val busStopDecoder: Decoder[BusStop] = deriveDecoder[BusStop]
    implicit val busPosDataDecoder: Decoder[BusPositionDataForTransmission] = deriveDecoder[BusPositionDataForTransmission]
    parse(str).right.value.as[List[BusPositionDataForTransmission]].right.value
  }
}