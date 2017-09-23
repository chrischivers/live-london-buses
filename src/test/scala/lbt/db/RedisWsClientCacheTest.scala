package lbt.db

import java.util.UUID

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.db.caching.RedisWsClientCache
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class RedisWsClientCacheTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
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
    val vehicleID1 = Random.nextString(10)
    val vehicleID2 = Random.nextString(10)
    val vehicleID3 = Random.nextString(10)
    f.redisClient.persistVehicleActivity(uuid, vehicleID1, System.currentTimeMillis()).futureValue
    f.redisClient.persistVehicleActivity(uuid, vehicleID2, System.currentTimeMillis() + 60000).futureValue
    f.redisClient.persistVehicleActivity(uuid, vehicleID3, System.currentTimeMillis() - 60000).futureValue

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    results should have size 3
    results.head shouldBe vehicleID3
    results(1) shouldBe vehicleID1
    results(2) shouldBe vehicleID2
  }

  test("When vehicle activity retrieved from Redis, it is purged") { f =>

    val uuid = UUID.randomUUID().toString
    val vehicleID1 = Random.nextString(10)
    val vehicleID2 = Random.nextString(10)
    f.redisClient.persistVehicleActivity(uuid, vehicleID1, System.currentTimeMillis() + 60000).futureValue
    f.redisClient.persistVehicleActivity(uuid, vehicleID2, System.currentTimeMillis()).futureValue

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    results should have size 2
    results.head shouldBe vehicleID2
    results(1) shouldBe vehicleID1

    val resultsAgain = f.redisClient.getVehicleActivityFor(uuid).futureValue
    resultsAgain should have size 0
  }

  test("When no vehicle activity in Redis for uuid, an empty result set is retrieved)") { f =>

    val uuid = UUID.randomUUID().toString

    val results = f.redisClient.getVehicleActivityFor(uuid).futureValue
    results should have size 0
  }

  test("Results coming back from Redis for uuid are limited to 100 per request") { f =>

    val uuid = UUID.randomUUID().toString
    Future.sequence((0 to 100).map { int =>
      f.redisClient.persistVehicleActivity(uuid, s"vehicle$int", System.currentTimeMillis() + Random.nextInt(60000))
    }).futureValue

    f.redisClient.getVehicleActivityFor(uuid).futureValue should have size 100
    f.redisClient.getVehicleActivityFor(uuid).futureValue should have size 1
    f.redisClient.getVehicleActivityFor(uuid).futureValue should have size 0
  }


  test("Vehicle activity for a uuid in Redis expires if nothing persisted in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val vehicleID1 = Random.nextString(10)
    val vehicleID2 = Random.nextString(10)
    f.redisClient.persistVehicleActivity(uuid, vehicleID1, System.currentTimeMillis()).futureValue
    f.redisClient.persistVehicleActivity(uuid, vehicleID2, System.currentTimeMillis()).futureValue
    Thread.sleep(5500)
    f.redisClient.getVehicleActivityFor(uuid).futureValue should have size 0
  }

  test("Vehicle activity for a uuid in Redis expires if no get requests received in in TTL period") { f =>

    val uuid = UUID.randomUUID().toString
    val vehicleID1 = Random.nextString(10)
    val vehicleID2 = Random.nextString(10)
    f.redisClient.persistVehicleActivity(uuid, vehicleID1, System.currentTimeMillis()).futureValue
    Thread.sleep(3000)
    f.redisClient.persistVehicleActivity(uuid, vehicleID2, System.currentTimeMillis()).futureValue
    Thread.sleep(3000)
    f.redisClient.getVehicleActivityFor(uuid).futureValue should have size 0
  }
}