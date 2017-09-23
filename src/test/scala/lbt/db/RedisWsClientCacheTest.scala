package lbt.db

import java.util.UUID

import akka.actor.ActorSystem
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import lbt.ConfigLoader
import lbt.db.caching.{BusPositionDataForTransmission, RedisWsClientCache}
import lbt.models.BusRoute
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class RedisWsClientCacheTest extends fixture.FunSuite with ScalaFutures with OptionValues with EitherValues {

  val config = ConfigLoader.defaultConfig

  implicit val busRouteDecoder: Decoder[BusRoute] = deriveDecoder[BusRoute]
  implicit val busPosDataDecoder: Decoder[BusPositionDataForTransmission] = deriveDecoder[BusPositionDataForTransmission]

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisClient: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisClient = new RedisWsClientCache(config.redisDBConfig.copy(dbIndex = 1, wsClientCacheTTL = 5 seconds)) // 1 = test, 0 = main
    val testFixture = FixtureParam(redisClient)

    try {
      redisClient.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisClient.flushDB.futureValue
    }
  }

  test("Vehicle activity persisted to Redis sorted set and retrieved ordered by timestamp (oldest first)") { f =>

    val uuid = UUID.randomUUID().toString
    val busPosData1 = createBusPositionData(timeStamp = System.currentTimeMillis())
    val busPosData2 = createBusPositionData(timeStamp = System.currentTimeMillis() + 60000)
    val busPosData3 = createBusPositionData(timeStamp = System.currentTimeMillis() - 60000)
    f.redisClient.storeVehicleActivity(uuid, busPosData1).futureValue
    f.redisClient.storeVehicleActivity(uuid, busPosData2).futureValue
    f.redisClient.storeVehicleActivity(uuid, busPosData3).futureValue

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    val parsedResults: List[BusPositionDataForTransmission] = parseRedisResult(results)
    parsedResults should have size 3
    parsedResults.head shouldBe busPosData3
    parsedResults(1)  shouldBe busPosData1
    parsedResults(2) shouldBe busPosData2
  }

  test("When vehicle activity retrieved from Redis, it is purged") { f =>

    val uuid = UUID.randomUUID().toString
    val busPosData1 = createBusPositionData(timeStamp = System.currentTimeMillis() + 60000)
    val busPosData2 = createBusPositionData(timeStamp = System.currentTimeMillis())
    f.redisClient.storeVehicleActivity(uuid, busPosData1).futureValue
    f.redisClient.storeVehicleActivity(uuid, busPosData2).futureValue

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    val parsedResults: List[BusPositionDataForTransmission] = parseRedisResult(results)
    parsedResults should have size 2
    parsedResults.head shouldBe busPosData2
    parsedResults(1) shouldBe busPosData1

    val resultsAgain = f.redisClient.getVehicleActivityFor(uuid).futureValue
    parseRedisResult(resultsAgain) should have size 0
  }

  test("When no vehicle activity in Redis for uuid, an empty result set is retrieved)") { f =>

    val uuid = UUID.randomUUID().toString

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    parseRedisResult(results) should have size 0
  }

  test("Results coming back from Redis for uuid are limited to 100 per request") { f =>

    val uuid = UUID.randomUUID().toString
    Future.sequence((0 to 100).map { int =>
      f.redisClient.storeVehicleActivity(uuid, createBusPositionData(timeStamp = System.currentTimeMillis() + Random.nextInt(60000)))
    }).futureValue


    parseRedisResult(f.redisClient.getVehicleActivityFor(uuid).futureValue) should have size 100
    parseRedisResult(f.redisClient.getVehicleActivityFor(uuid).futureValue) should have size 1
    parseRedisResult(f.redisClient.getVehicleActivityFor(uuid).futureValue) should have size 0
  }


  test("Vehicle activity for a uuid in Redis expires if nothing persisted in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val busPos1 = createBusPositionData()
    val busPos2 = createBusPositionData()

    f.redisClient.storeVehicleActivity(uuid, busPos1).futureValue
    f.redisClient.storeVehicleActivity(uuid, busPos2).futureValue
    Thread.sleep(5500)
    parseRedisResult(f.redisClient.getVehicleActivityFor(uuid).futureValue) should have size 0
  }

  test("Vehicle activity for a uuid in Redis expires if no get requests received in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val busPos1 = createBusPositionData()
    val busPos2 = createBusPositionData()

    f.redisClient.storeVehicleActivity(uuid, busPos1).futureValue
    Thread.sleep(3000)
    f.redisClient.storeVehicleActivity(uuid, busPos2).futureValue
    Thread.sleep(3000)
    parseRedisResult(f.redisClient.getVehicleActivityFor(uuid).futureValue) should have size 0
  }

  private def createBusPositionData(vehicleId: String = Random.nextString(10),
                                    busRoute: BusRoute = BusRoute("3", "outbound"),
                                    lat: Double = 51.4217,
                                    lng: Double = -0.077507,
                                    nextStopName: String = "NextStop",
                                    timeStamp: Long = System.currentTimeMillis()) = {
    BusPositionDataForTransmission(vehicleId, busRoute, lat, lng, nextStopName, timeStamp)
  }

  private def parseRedisResult(str: String): List[BusPositionDataForTransmission] = {
    parse(str).right.value.as[List[BusPositionDataForTransmission]].right.value
  }
}