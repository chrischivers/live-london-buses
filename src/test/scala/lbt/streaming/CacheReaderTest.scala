package lbt.streaming

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import lbt.common.{Commons, Definitions}
import lbt.db.caching._
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusRoute, LatLng, LatLngBounds}
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.web.FilteringParams
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CacheReaderTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll with Eventually {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second)
  )

  implicit val akkaTimeout = Timeout(3, TimeUnit.SECONDS)

  //These are set outside the fixture so that definitions db does not need to be repopulated before each test
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound"), BusRoute("3", "outbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)

  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(definitions: Definitions, sourceLineHandler: SourceLineHandler, redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache, cacheReader: ActorRef)

  def withFixture(test: OneArgTest) = {
    implicit val actorSystem = ActorSystem()
    val definitions = new Definitions(routeDefinitionsTable)
    val redisConfig = config.redisDBConfig.copy(dbIndex = 1)

    val redisArrivalTimeLog = new RedisArrivalTimeLog(redisConfig) // 1 = test, 0 = main
    val redisVehicleArrivalTimeLog = new RedisVehicleArrivalTimeLog(redisConfig, config.streamingConfig)
    val redisSubscriberCache = new RedisSubscriberCache(redisConfig)
    val redisWsClientCache = new RedisWsClientCache(redisConfig, redisSubscriberCache)
    val sourceLineHandler = new SourceLineHandler(redisArrivalTimeLog, redisVehicleArrivalTimeLog, definitions, config.streamingConfig)
    val cacheReader = actorSystem.actorOf(Props(new CacheReader(redisArrivalTimeLog, redisVehicleArrivalTimeLog, redisSubscriberCache, redisWsClientCache, definitions)))

    val testFixture = FixtureParam(definitions, sourceLineHandler, redisWsClientCache, redisSubscriberCache, cacheReader)

    try {
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    } finally {
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }


    test("Cache reader sends records to web socket client cache (no next stop information available)") { f =>

      val sourceLine = generateSourceLine()
      val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

      val uuid = UUID.randomUUID().toString
      val params = FilteringParams(List(busRoute), LatLngBounds(LatLng(50, -1), LatLng(52, 1)))
      f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

      f.sourceLineHandler.handle(sourceLine).futureValue

      parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value should have size 0

      f.cacheReader ! CacheReadCommand(40000)

      eventually {
        val results = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value
        results should have size 1
        results.head shouldBe BusPositionDataForTransmission(
          sourceLine.vehicleId,
          busRoute,
          getBusStopFromStopID(sourceLine.stopID, definitions).get,
          sourceLine.arrivalTimeStamp,
          getNextBusStopFromStopID(sourceLine.stopID, busRoute, definitions).map(_.stopName),
          None,
          None)
      }
  }

  test("Cache reader sends records to web socket client cache (with next stop information available)") { f =>

    val busRoute = BusRoute("25", "outbound")
    val definitionsForRoute = definitions.routeDefinitions(busRoute)
    val timestamp1 = System.currentTimeMillis() + 40000
    val timestamp2 = System.currentTimeMillis() + 60000
    val sourceLine1 = generateSourceLine(stopId = definitionsForRoute(5)._2.stopID, timeStamp = timestamp1)
    val sourceLine2 = generateSourceLine(stopId = definitionsForRoute(6)._2.stopID, timeStamp = timestamp2)

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(busRoute), LatLngBounds(LatLng(50, -1), LatLng(52, 1)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    f.sourceLineHandler.handle(sourceLine1).futureValue
    f.sourceLineHandler.handle(sourceLine2).futureValue

    f.cacheReader ! CacheReadCommand(60001)

    eventually {
      val results = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value
      results should have size 2
      results.head shouldBe BusPositionDataForTransmission(
        sourceLine1.vehicleId,
        busRoute,
        getBusStopFromStopID(sourceLine1.stopID, definitions).get,
        sourceLine1.arrivalTimeStamp,
        getNextBusStopFromStopID(sourceLine1.stopID, busRoute, definitions).map(_.stopName),
        Some(timestamp2),
        None)

      results(1) shouldBe BusPositionDataForTransmission(
        sourceLine2.vehicleId,
        busRoute,
        getBusStopFromStopID(sourceLine2.stopID, definitions).get,
        sourceLine2.arrivalTimeStamp,
        getNextBusStopFromStopID(sourceLine2.stopID, busRoute, definitions).map(_.stopName),
        None,
        None)
    }
  }

  test("user only receives source lines for routes they are subscribed to, when within bounds") { f =>
    val subscribedBusRoute = BusRoute("25", "outbound")
    val sourceLine1 = generateSourceLine(route = "25", direction = 1)
    val sourceLine2 = generateSourceLine(route = "3", direction = 1, stopId = "490006864S1")

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(subscribedBusRoute), LatLngBounds(LatLng(50, -1), LatLng(52, 1)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    f.sourceLineHandler.handle(sourceLine1).futureValue
    f.sourceLineHandler.handle(sourceLine2).futureValue

    parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value should have size 0

    f.cacheReader ! CacheReadCommand(40000)

    eventually {
      val results = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value
      results should have size 1
      results.head shouldBe BusPositionDataForTransmission(
        sourceLine1.vehicleId,
        subscribedBusRoute,
        getBusStopFromStopID(sourceLine1.stopID, definitions).get,
        sourceLine1.arrivalTimeStamp,
        getNextBusStopFromStopID(sourceLine1.stopID, subscribedBusRoute, definitions).map(_.stopName),
        None,
        None)
    }
  }

  test("user not subscribed receives no source lines") { f =>
    val sourceLine1 = generateSourceLine()

    val uuid = UUID.randomUUID().toString
    f.redisSubscriberCache.subscribe(uuid, None).futureValue

    f.sourceLineHandler.handle(sourceLine1).futureValue

    f.cacheReader ! CacheReadCommand(40000)
    Thread.sleep(2000)
    val results = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value
    results should have size 0
  }

  test("user receives no source lines when they are subscribed to route ut not in bounds") { f =>
    val subscribedBusRoute = BusRoute("25", "outbound")
    val sourceLine1 = generateSourceLine(route = "25", direction = 1)

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(subscribedBusRoute), LatLngBounds(LatLng(53, -1), LatLng(54, 1)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    f.sourceLineHandler.handle(sourceLine1).futureValue

    f.cacheReader ! CacheReadCommand(40000)
    Thread.sleep(2000)
    val results = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityJsonFor(uuid).futureValue).value
    results should have size 0
  }
}
