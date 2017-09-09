package lbt.db

import lbt.ConfigLoader
import lbt.models.BusRoute
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.duration._

class RedisDbTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(redisClient: RedisClient)

  def withFixture(test: OneArgTest) = {
    val redisClient = new RedisClient(config.redisDBConfig.copy(dbName = "1")) // 1 = test, 0 = main

    val testFixture = FixtureParam(redisClient)

    try {
      redisClient.flushDB
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisClient.flushDB
    }
  }

  test("Route timing data persisted to redis should match those retrieved") { f =>

    val busRoute = BusRoute("3", "outbound")
    val time1 = 60
    val time2 = 65

    f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, time1)
    f.redisClient.persistStopToStopTime(busRoute, 0, 1, 0, time2)

    val timeList = f.redisClient.getStopToStopTimes(busRoute, 0, 1).value
    timeList should have size 2
    timeList shouldBe List(time2, time1)
  }

}