package lbt.db

import akka.actor.ActorSystem
import lbt.db.caching.{RedisArrivalTimeLog, RedisVehicleArrivalTimeLog}
import lbt.{ConfigLoader, DefinitionsConfig, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RedisVehicleArrivalTimeLogTest extends fixture.FunSuite with ScalaFutures with OptionValues with SharedTestFeatures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisVehicleArrivalTimeLog = new RedisVehicleArrivalTimeLog(
      config.redisConfig.copy(dbIndex = 1),
      config.streamingConfig.copy(idleTimeBeforeVehicleDeleted = 5 seconds)
    ) // 1 = test, 0 = main
    val testFixture = FixtureParam(redisVehicleArrivalTimeLog)

    try {
      redisVehicleArrivalTimeLog.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisVehicleArrivalTimeLog.flushDB.futureValue
    }
  }

  test("An arrival time for a vehicle is added to cache") { f =>

    val arrivalTime = System.currentTimeMillis() + 120000
    val stopArrivalRecord = generateStopArrivalRecord()

    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord, arrivalTime).futureValue

    val result = f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).futureValue
    result should have size 1
    result.head.stopIndex shouldBe stopArrivalRecord.stopIndex
    result.head.arrivalTime shouldBe arrivalTime
  }

  test("An updated arrival time for a vehicle is replaced in the cache") { f =>

    val arrivalTime1 = System.currentTimeMillis() + 120000
    val arrivalTime2 = System.currentTimeMillis() + 160000
    val stopArrivalRecord = generateStopArrivalRecord()

    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord, arrivalTime1).futureValue

    val result = f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).futureValue
    result should have size 1
    result.head.arrivalTime shouldBe arrivalTime1

    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord, arrivalTime2).futureValue

    val finalResult = f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).futureValue
    finalResult should have size 1
    finalResult.head.arrivalTime shouldBe arrivalTime2
  }

  test("Multiple indexes for a route are persisted to cache") { f =>

    val arrivalTime = System.currentTimeMillis() + 120000
    val stopArrivalRecord1 = generateStopArrivalRecord(stopIndex = 0)
    val stopArrivalRecord2 = generateStopArrivalRecord(stopIndex = 1)

    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord1, arrivalTime).futureValue
    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord2, arrivalTime).futureValue

    val result = f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord1.vehicleId, stopArrivalRecord1.busRoute).futureValue
    result should have size 2
    result.head.arrivalTime shouldBe arrivalTime
    result.head.stopIndex shouldBe 0
    result(1).arrivalTime shouldBe arrivalTime
    result(1).stopIndex shouldBe 1
  }

  test("Vehicles expire if no activity for a specified period") { f =>

    val arrivalTime = System.currentTimeMillis() + 120000
    val stopArrivalRecord = generateStopArrivalRecord()

    f.redisVehicleArrivalTimeLog.addVehicleArrivalTime(stopArrivalRecord, arrivalTime).futureValue

    f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).futureValue should have size 1
    Thread.sleep(5500)
    f.redisVehicleArrivalTimeLog.getArrivalTimes(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute).futureValue should have size 0
  }

}