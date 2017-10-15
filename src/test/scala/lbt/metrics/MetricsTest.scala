package lbt.metrics

import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MetricsTest extends FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with EitherValues with BeforeAndAfterAll with Eventually {

  test("Source Lines read metrics meter increases when called") {
    TestMetricsLogging.metrics.meter("source-lines-received").count shouldBe 0
    TestMetricsLogging.incrSourceLinesReceived
    TestMetricsLogging.metrics.meter("source-lines-received").count shouldBe 1
  }
  test("Source Lines validated metrics meter increases when called") {
    TestMetricsLogging.metrics.meter("source-lines-validated").count shouldBe 0
    TestMetricsLogging.incrSourceLinesValidated
    TestMetricsLogging.metrics.meter("source-lines-validated").count shouldBe 1
  }
  test("Arrival Times logged metrics meter increases when called") {
    TestMetricsLogging.metrics.meter("arrival-times-logged").count shouldBe 0
    TestMetricsLogging.incrArrivalTimesLogged
    TestMetricsLogging.metrics.meter("arrival-times-logged").count shouldBe 1
  }

  test("Vehicle arrival times logged metrics meter increases when called") {
    TestMetricsLogging.metrics.meter("vehicle-arrival-times-logged").count shouldBe 0
    TestMetricsLogging.incrVehicleArrivalTimesLogged
    TestMetricsLogging.metrics.meter("vehicle-arrival-times-logged").count shouldBe 1
  }
  test("Cached records processed metrics meter increases when called") {
    TestMetricsLogging.metrics.meter("cached-records-processed").count shouldBe 0
    TestMetricsLogging.incrCachedRecordsProcessed(3)
    TestMetricsLogging.metrics.meter("cached-records-processed").count shouldBe 3
  }

  test("Cached read processing timer metric times the length of the future") {
    TestMetricsLogging.measureCacheReadProcess {
      Future(Thread.sleep(2000))
    }
    Thread.sleep(3000)
    (TestMetricsLogging.metrics.timer("cache-read-processing").mean / 1000 / 1000 / 1000).toInt shouldBe 2
  }

  test("Users connected to WS counter increments/decrements when called") {
    TestMetricsLogging.metrics.counter("users-connected-ws").count shouldBe 0
    TestMetricsLogging.incrUsersConnectedToWs
    TestMetricsLogging.metrics.counter("users-connected-ws").count shouldBe 1
    TestMetricsLogging.decrUsersConnectedToWs
    TestMetricsLogging.metrics.counter("users-connected-ws").count shouldBe 0
  }

}

object TestMetricsLogging extends MetricsLogging {
  override val metricsConfig = ConfigLoader.defaultConfig.metricsConfig.copy(enabled = true)
  setUpReporter
}