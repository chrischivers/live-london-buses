package lbt.db

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.db.caching.RedisArrivalTimeLog
import lbt.models.BusRoute
import lbt.streaming.StopArrivalRecord
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RedisArrivalTimeLogTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisArrivalTimeLog: RedisArrivalTimeLog)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisArrivalTimeLog = new RedisArrivalTimeLog(config.redisDBConfig.copy(dbIndex = 1)) // 1 = test, 0 = main
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

    val vehicleId = "A123456"
    val busRoute = BusRoute("3", "outbound")
    val stopSeq = 2
    val arrivalTime = System.currentTimeMillis() + 120000

    val stopArrivalRecord = StopArrivalRecord(vehicleId, busRoute, stopSeq)

    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime, stopArrivalRecord).futureValue
    val result = f.redisArrivalTimeLog.getArrivalRecords(arrivalTime + 1).futureValue
    result should have size 1
    result.head._1 shouldBe stopArrivalRecord
    result.head._2 shouldBe arrivalTime
  }

  test("When arrival time is updated, cache retains most recent addition") { f =>

    val vehicleId = "A123456"
    val busRoute = BusRoute("3", "outbound")
    val stopSeq = 2
    val arrivalTime1 = System.currentTimeMillis() + 120000
    val arrivalTime2 = System.currentTimeMillis() + 160000

    val stopArrivalRecord1 = StopArrivalRecord(vehicleId, busRoute, stopSeq)
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime1, stopArrivalRecord1).futureValue

    val stopArrivalRecord2 = StopArrivalRecord(vehicleId, busRoute, stopSeq)
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime2, stopArrivalRecord2).futureValue

    val result = f.redisArrivalTimeLog.getArrivalRecords(arrivalTime2 + 1).futureValue
    result should have size 1
    result.head._1 shouldBe stopArrivalRecord2
    result.head._2 shouldBe arrivalTime2
  }


  test("When arrival records are retrieved, they are wiped from the cache") { f =>

    val vehicleId = "A123456"
    val busRoute = BusRoute("3", "outbound")
    val stopSeq = 2
    val arrivalTime = System.currentTimeMillis() + 120000

    val stopArrivalRecord = StopArrivalRecord(vehicleId, busRoute, stopSeq)
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTime, stopArrivalRecord).futureValue


    val result1 = f.redisArrivalTimeLog.getArrivalRecords(arrivalTime + 1).futureValue
    result1 should have size 1

    val result2 = f.redisArrivalTimeLog.getArrivalRecords(arrivalTime + 1).futureValue
    result2 should have size 0
  }
}