package lbt.metrics

import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures


class MetricsTest extends FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with EitherValues with BeforeAndAfterAll {


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
  
}

object TestMetricsLogging extends MetricsLogging {
  override val metricsConfig = ConfigLoader.defaultConfig.metricsConfig.copy(enabled = true)
  setUpReporter
}