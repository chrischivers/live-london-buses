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

  private val cachedRecordsProcessed: Meter = metrics.meter("cached-records-processed")
  def incrCachedRecordsProcessed(n: Int) = if (metricsConfig.enabled) cachedRecordsProcessed.mark(n)

  private val cacheReadProcessingTimer: Timer = metrics.timer("cache-read-processing-time")
  def measureCacheReadProcess[A](f: => Future[A]) =
    if (metricsConfig.enabled) cacheReadProcessingTimer.timeFuture(f) else f

  private val mapHttpRequestsReceived: Meter = metrics.meter("map-http-requests-received")
  def incrMapHttpRequestsReceived = if (metricsConfig.enabled) mapHttpRequestsReceived.mark()

  private val nextStopsHttpRequestsReceived: Meter = metrics.meter("next-stops-http-requests-received")
  def incrNextStopsHttpRequestsReceived = if (metricsConfig.enabled) nextStopsHttpRequestsReceived.mark()


  private val snapshotHttpRequestsReceived: Meter = metrics.meter("snapshot-http-requests-received")
  def incrSnapshotHttpRequestsReceived = if (metricsConfig.enabled) snapshotHttpRequestsReceived.mark()

  private val usersCurrentlySubscribed: Counter = metrics.counter("users-subscribed")
  def setUsersCurrentlySubscribed(n: Int) = if (metricsConfig.enabled) {
    usersCurrentlySubscribed.dec(usersCurrentlySubscribed.count)
    usersCurrentlySubscribed.inc(n)
  }
}

object MetricsLogging extends MetricsLogging {
  setUpReporter
}