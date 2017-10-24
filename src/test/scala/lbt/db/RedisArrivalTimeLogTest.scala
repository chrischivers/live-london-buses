package lbt.db

import akka.actor.ActorSystem
import lbt.db.caching.RedisArrivalTimeLog
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RedisArrivalTimeLogTest extends fixture.FunSuite with ScalaFutures with OptionValues with SharedTestFeatures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisArrivalTimeLog: RedisArrivalTimeLog)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisArrivalTimeLog = new RedisArrivalTimeLog(config.redisConfig.copy(dbIndex = 1)) // 1 = test, 0 = main
    val testFixture = FixtureParam(redisArrivalTimeLog)

    try {
      redisArrivalTimeLog.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisArrivalTimeLog.flushDB.futureValue
    }
  }

  test("A new arrival time is added to cache") { f =>

    val arrivalTime = System.currentTimeMillis() + 120000
    val stopArrivalRecord = generateStopArrivalRecord()

    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime, stopArrivalRecord).futureValue
    val result = f.redisArrivalTimeLog.takeArrivalRecordsUpTo(arrivalTime + 1).futureValue
    result should have size 1
    result.head._1 shouldBe stopArrivalRecord
    result.head._2 shouldBe arrivalTime
  }

  test("When arrival time is updated, cache retains most recent addition") { f =>

    val arrivalTime1 = System.currentTimeMillis() + 120000
    val arrivalTime2 = System.currentTimeMillis() + 160000

    val stopArrivalRecord1 = generateStopArrivalRecord()
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime1, stopArrivalRecord1).futureValue

    val stopArrivalRecord2 = generateStopArrivalRecord()
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime2, stopArrivalRecord2).futureValue

    val result = f.redisArrivalTimeLog.takeArrivalRecordsUpTo(arrivalTime2 + 1).futureValue
    result should have size 1
    result.head._1 shouldBe stopArrivalRecord2
    result.head._2 shouldBe arrivalTime2
  }


  test("When arrival records are retrieved, they are wiped from the cache") { f =>

    val arrivalTime1 = System.currentTimeMillis() + 120000
    val stopArrivalRecord1 = generateStopArrivalRecord(vehicleId = "Vehicle1")

    val arrivalTime2 = System.currentTimeMillis() + 120001
    val stopArrivalRecord2 = generateStopArrivalRecord(vehicleId = "Vehicle2")

    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime1, stopArrivalRecord1).futureValue
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime2, stopArrivalRecord2).futureValue

    val result1 = f.redisArrivalTimeLog.takeArrivalRecordsUpTo(arrivalTime1).futureValue
    result1 should have size 1
    result1.head._1 shouldBe stopArrivalRecord1
    result1.head._2 shouldBe arrivalTime1

    val result2 = f.redisArrivalTimeLog.takeArrivalRecordsUpTo(arrivalTime1).futureValue
    result2 should have size 0

    val result3 = f.redisArrivalTimeLog.takeArrivalRecordsUpTo(arrivalTime2).futureValue
    result3 should have size 1
    result3.head._1 shouldBe stopArrivalRecord2
    result3.head._2 shouldBe arrivalTime2
  }
}