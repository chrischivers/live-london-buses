package lbt.db

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.db.caching.RedisDurationRecorder
import lbt.models.BusRoute
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class RedisDurationRecorderTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisClient: RedisDurationRecorder)

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisClient = new RedisDurationRecorder(config.redisDBConfig.copy(dbIndex = 1, durationRecordTTL = 5 seconds)) // 1 = test, 0 = main
    val testFixture = FixtureParam(redisClient)

    try {
      redisClient.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisClient.flushDB.futureValue
    }
  }

  test("Route timing data persisted to redis should match those retrieved") { f =>

    val busRoute = BusRoute("3", "outbound")
    val time1 = 60
    val time2 = 65

    f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, time1).futureValue
    f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, time2).futureValue

    val timeList = f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue
    timeList should have size 2
    timeList shouldBe List(time2, time1)
  }

  test("Route timing data stored in redis should be limited by maxListLength") { f =>

    val busRoute = BusRoute("3", "outbound")

    Future.sequence((0 to 98).map(_ => f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, Random.nextInt(200)))).futureValue
    f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue should have size 99

    f.redisClient.flushDB.futureValue

    Future.sequence((0 to 99).map(_ => f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, Random.nextInt(200)))).futureValue
    f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue should have size 100

    f.redisClient.flushDB.futureValue

    Future.sequence((0 to 100).map(_ => f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, Random.nextInt(200)))).futureValue
    f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue should have size 100
  }

  test("Route timing data stored in redis should expire after specified period passed") { f =>

    val busRoute = BusRoute("3", "outbound")
    f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, Random.nextInt(200)).futureValue
    f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue should have size 1
    Thread.sleep(5500)
    f.redisClient.getStopToStopTimes(busRoute, 0, 1).futureValue should have size 0
  }
}