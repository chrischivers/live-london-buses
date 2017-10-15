package lbt.metrics

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import lbt.{ConfigLoader, MetricsConfig}
import metrics_influxdb.{HttpInfluxdbProtocol, InfluxdbReporter}
import nl.grons.metrics.scala._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

trait MetricsLogging extends StrictLogging with DefaultInstrumented {

  override lazy val metricBaseName: MetricName = MetricName("")

  val metricsConfig: MetricsConfig = ConfigLoader.defaultConfig.metricsConfig

  def setUpReporter = {
    if (metricsConfig.enabled) {
      logger.info("Setting up metrics reporter")
      InfluxdbReporter.forRegistry(metricRegistry)
        .protocol(new HttpInfluxdbProtocol(metricsConfig.host, metricsConfig.port, metricsConfig.dbName))
        .tag("hostname", InetAddress.getLocalHost.getHostName)
        .convertRatesTo(TimeUnit.MINUTES)
        .build().start(metricsConfig.updateInterval, TimeUnit.SECONDS)
    }
  }

  private val sourceLinesReceivedMeter: Meter = metrics.meter("source-lines-received")
  def incrSourceLinesReceived = if (metricsConfig.enabled) sourceLinesReceivedMeter.mark()

  private val sourceLinesValidated: Meter = metrics.meter("source-lines-validated")
  def incrSourceLinesValidated = if (metricsConfig.enabled) sourceLinesValidated.mark()

  private val arrivalTimesLoggedToCache: Meter = metrics.meter("arrival-times-logged")
  def incrArrivalTimesLogged = if (metricsConfig.enabled) arrivalTimesLoggedToCache.mark()

  private val vehicleArrivalTimesLoggedToCache: Meter = metrics.meter("vehicle-arrival-times-logged")
  def incrVehicleArrivalTimesLogged = if (metricsConfig.enabled) vehicleArrivalTimesLoggedToCache.mark()

  private val cachedRecordsProcessed: Meter = metrics.meter("cached-records-processed")
  def incrCachedRecordsProcessed(n: Int) = if (metricsConfig.enabled) cachedRecordsProcessed.mark(n)

  private val cacheReadProcessingTimer: Timer = metrics.timer("cache-read-processing")
  def measureCacheReadProcess[A](f: => Future[A]) =
    if (metricsConfig.enabled) cacheReadProcessingTimer.timeFuture(f) else f

  private val usersConnectedToWs: Counter = metrics.counter("users-connected-ws")
  def incrUsersConnectedToWs = if (metricsConfig.enabled)  usersConnectedToWs.inc()
  def decrUsersConnectedToWs = if (metricsConfig.enabled)  usersConnectedToWs.dec()

}

object MetricsLogging extends MetricsLogging {
  setUpReporter
}