package lbt.streaming

import java.util.UUID

import akka.actor.ActorSystem
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import lbt.common.{Commons, Definitions}
import lbt.db.caching.{BusPositionDataForTransmission, RedisDurationRecorder, RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusRoute, BusStop, LatLng, LatLngBounds}
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.web.{FilteringParams, WebSocketClientHandler}
import lbt.{ConfigLoader, LBTConfig, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues, fixture}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalacache.guava.GuavaCache
import scalacache.{NoSerialization, ScalaCache, _}

class SourceLineHandlerTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with EitherValues with BeforeAndAfterAll {

  val config: LBTConfig = ConfigLoader.defaultConfig
  val configWithShortTtl: LBTConfig = config.copy(sourceLineHandlerConfig = config.sourceLineHandlerConfig.copy(cacheTtl = 5 seconds))
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(1 minutes),
    interval = scaled(1 second)
  )

  //These are set outside the fixture so that definitions db does not need to be repopulated before each test
  val db = new PostgresDB(configWithShortTtl.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(configWithShortTtl.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)

  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(sourceLineHandler: SourceLineHandler, cache: ScalaCache[NoSerialization], redisDurationRecorder: RedisDurationRecorder, redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache)

  def withFixture(test: OneArgTest) = {

    val cache = ScalaCache(GuavaCache())
    implicit val actorSystem: ActorSystem = ActorSystem()
    val modifiedConfig = config.redisDBConfig.copy(dbIndex = 1) // 1 = test, 0 = main

    val redisDurationRecorder = new RedisDurationRecorder(modifiedConfig)
    val redisSubscriberCache = new RedisSubscriberCache(modifiedConfig)
    val redisWsClientCache = new RedisWsClientCache(modifiedConfig, redisSubscriberCache)

    val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)


    val sourceLineHandler = new SourceLineHandler(definitions, configWithShortTtl.sourceLineHandlerConfig, redisDurationRecorder, webSocketClientHandler)(cache, ec)
    val testFixture = FixtureParam(sourceLineHandler, cache, redisDurationRecorder, redisSubscriberCache, redisWsClientCache)

    try {
      redisDurationRecorder.flushDB.futureValue
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    } finally {
      redisDurationRecorder.flushDB.futureValue
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
    }
  }

  test("Source Line handled is persisted to cache") { f =>
    val timestamp = System.currentTimeMillis() + 10000
    val sourceLine = generateSourceLine(timeStamp = timestamp)
    f.sourceLineHandler.handle(sourceLine).value.futureValue
    val timeStampFromCache = getArrivalTimestampFromCache(sourceLine, f.cache).futureValue.value
    timeStampFromCache shouldBe timestamp
  }

  test("Source Line handled is persisted to websocket cache (when user is subscribed") { f =>

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(BusRoute("25", "outbound")),LatLngBounds(LatLng(51,52), LatLng(52,53)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    val timestamp = System.currentTimeMillis() + 10000
    val sourceLine = generateSourceLine(timeStamp = timestamp)
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    f.sourceLineHandler.handle(sourceLine).value.futureValue

    val dataForTransmission = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityFor(uuid).futureValue).value
    dataForTransmission should have size 1
    dataForTransmission.head.vehicleId shouldBe sourceLine.vehicleID
    dataForTransmission.head.busRoute shouldBe busRoute
    dataForTransmission.head.arrivalTimestamp shouldBe timestamp
    dataForTransmission.head.busStop shouldBe definitions.routeDefinitions.get(busRoute).value.find(_._2.stopID == sourceLine.stopID).value._2
    dataForTransmission.head.avgTimeToNextStop.value shouldBe 0
    //TODO test for additional data (next stop name etc)
  }

  test("Source Lines are persisted to websocket cache and average is obtained") { f =>

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(BusRoute("25", "outbound")),LatLngBounds(LatLng(51,52), LatLng(52,53)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    val busRoute = BusRoute("25", "outbound")
    val fromStop = definitions.routeDefinitions(busRoute)(3)._2
    val toStop = definitions.routeDefinitions(busRoute)(4)._2
    val fromTimestamp = System.currentTimeMillis() + 10000
    val sourceLine1 = generateSourceLine(vehicleId = "VEHICLEREG1", timeStamp = fromTimestamp, stopId = fromStop.stopID)
    val sourceLine2 = generateSourceLine(vehicleId = "VEHICLEREG1", timeStamp = fromTimestamp + 60000, stopId = toStop.stopID)
    val sourceLine3 = generateSourceLine(vehicleId = "VEHICLEREG2", timeStamp = fromTimestamp + 120000, stopId = fromStop.stopID)
    val sourceLine4 = generateSourceLine(vehicleId = "VEHICLEREG2", timeStamp = fromTimestamp + 160000, stopId = toStop.stopID)
    val sourceLine5 = generateSourceLine(vehicleId = "VEHICLEREG3", timeStamp = fromTimestamp + 180000, stopId = fromStop.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    f.sourceLineHandler.handle(sourceLine3).value.futureValue
    f.sourceLineHandler.handle(sourceLine4).value.futureValue
    f.sourceLineHandler.handle(sourceLine5).value.futureValue

    val dataForTransmission = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityFor(uuid).futureValue).value
    dataForTransmission should have size 5
    dataForTransmission(0).avgTimeToNextStop.value shouldBe 0
    dataForTransmission(1).avgTimeToNextStop.value shouldBe 0
    dataForTransmission(2).avgTimeToNextStop.value shouldBe 60
    dataForTransmission(3).avgTimeToNextStop.value shouldBe 0
    dataForTransmission(4).avgTimeToNextStop.value shouldBe 50
  }

  test("Where the stop is the last one, the 'time to next stop' and 'next stop name' field are null in the json") { f =>

    val uuid = UUID.randomUUID().toString
    val params = FilteringParams(List(BusRoute("25", "outbound")),LatLngBounds(LatLng(51,52), LatLng(52,53)))
    f.redisSubscriberCache.subscribe(uuid, Some(params)).futureValue

    val busRoute = BusRoute("25", "outbound")
    val fromStop = definitions.routeDefinitions(busRoute).last._2
    val fromTimestamp = System.currentTimeMillis() + 10000
    val sourceLine = generateSourceLine(vehicleId = "VEHICLEREG1", timeStamp = fromTimestamp, stopId = fromStop.stopID)
    f.sourceLineHandler.handle(sourceLine).value.futureValue

    val json = f.redisWsClientCache.getVehicleActivityFor(uuid).futureValue
    parseWebsocketCacheResult(json).value should have size 1
    println(json)
    val jsoncursor = parse(json).right.value.hcursor
//    jsoncursor.downField("avgTimeToNextStop").get[Int] shouldBe null
  }

  test("Source Line handled is not persisted to websocket cache (when user not subscribed") { f =>

    val uuid = UUID.randomUUID().toString
    f.redisSubscriberCache.subscribe(uuid, None).futureValue

    val sourceLine = generateSourceLine()
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    f.sourceLineHandler.handle(sourceLine).value.futureValue

    val dataForTransmission = parseWebsocketCacheResult(f.redisWsClientCache.getVehicleActivityFor(uuid).futureValue).value
    dataForTransmission should have size 0
  }

  test("When another source line arrives for a record already in cache, cache is updated with the most recent") { f =>
    val timeStamp1 = System.currentTimeMillis() + 10000
    val sourceLine1 = generateSourceLine(timeStamp = timeStamp1)
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val timeStamp2 = System.currentTimeMillis() + 5000
    val sourceLine2 = generateSourceLine(timeStamp = timeStamp2)
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    val timeStampFromCache = getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value
    timeStampFromCache shouldBe timeStamp2
  }

  test("Cache differentiates between the same stops for different vehicles") { f =>
    val timeStamp1 = System.currentTimeMillis() + 60000
    val sourceLine1 = generateSourceLine(timeStamp = timeStamp1, vehicleId = "VEHICLE1")
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val timeStamp2 = System.currentTimeMillis() + 120000
    val sourceLine2 = generateSourceLine(timeStamp = timeStamp2, vehicleId = "VEHICLE2")
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timeStamp1
    getArrivalTimestampFromCache(sourceLine2, f.cache).futureValue.value shouldBe timeStamp2
  }

  test("Cache is updated with last index persisted and persisted record removed") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(4)
    val sourceLine1 = generateSourceLine(timeStamp = timestamp1, stopId = stop1._2.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timestamp1
    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue should not be defined

    val timestamp2 = timestamp1 + 60000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(5)
    val sourceLine2 = generateSourceLine(timeStamp = timestamp2, stopId = stop2._2.stopID)

    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe 5

    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined
    getArrivalTimestampFromCache(sourceLine2, f.cache).futureValue.value shouldBe timestamp2
  }

  test("Incoming source line is ignored if index is equal or less than last persisted record") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(4)
    val sourceLine1 = generateSourceLine(timeStamp = timestamp1, stopId = stop1._2.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timestamp1

    val timestamp2 = timestamp1 + 60000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(5)
    val sourceLine2 = generateSourceLine(timeStamp = timestamp2, stopId = stop2._2.stopID)

    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe 5

    val timestamp3 = timestamp1 + 50000
    val stop3 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(5)
    val sourceLine3 = generateSourceLine(timeStamp = timestamp3, stopId = stop3._2.stopID)

    getArrivalTimestampFromCache(sourceLine3, f.cache).futureValue.value shouldBe timestamp2 //timestamp 3 disregarded

    val timestamp4 = timestamp1 + 2000
    val stop4 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(4)
    val sourceLine4 = generateSourceLine(timeStamp = timestamp4, stopId = stop4._2.stopID)

    getArrivalTimestampFromCache(sourceLine4, f.cache).futureValue should not be defined //timestamp4 disregarded
  }

  test("Stops at beginning of route are handled correctly") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value.head
    val sourceLine1 = generateSourceLine(timeStamp = timestamp1, stopId = stop1._2.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timestamp1

    val timestamp2 = timestamp1 + 60000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(1)
    val sourceLine2 = generateSourceLine(timeStamp = timestamp2, stopId = stop2._2.stopID)

    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe 1
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined

    f.sourceLineHandler.handle(sourceLine1).value.futureValue //try persisting previous index again
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe 1
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined
  }

  test("Stops at end of route are handled correctly") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val definitionsSize = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value.size

    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(definitionsSize - 2)
    val sourceLine1 = generateSourceLine(timeStamp = timestamp1, stopId = stop1._2.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timestamp1

    val timestamp2 = timestamp1 + 60000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(definitionsSize - 1)
    val sourceLine2 = generateSourceLine(timeStamp = timestamp2, stopId = stop2._2.stopID)

    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe (definitionsSize - 1)
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined

    f.sourceLineHandler.handle(sourceLine1).value.futureValue //try persisting previous index again
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe (definitionsSize - 1)
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined
  }

  test("Arrival Timestamp Records are dropped automatically if too much time elapses") { f =>
    val timestamp = System.currentTimeMillis() + 10000
    val sourceLine = generateSourceLine(timeStamp = timestamp)
    f.sourceLineHandler.handle(sourceLine).value.futureValue
    getArrivalTimestampFromCache(sourceLine, f.cache).futureValue.value shouldBe timestamp
    Thread.sleep(5500)
    getArrivalTimestampFromCache(sourceLine, f.cache).futureValue should not be defined
  }

  test("Last Index Persisted Records are dropped automatically if too much time elapses") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(9)
    val sourceLine1 = generateSourceLine(stopId = stop1._2.stopID, timeStamp = timestamp1)
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val timestamp2 = timestamp1 + 60000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(10)
    val sourceLine2 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = timestamp2)
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue.value shouldBe 10
    Thread.sleep(5500)
    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue should not be defined
  }

  test("Record not persisted if time difference is below threshold") { f =>
    val startingArrivalTime = System.currentTimeMillis() + 60000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(9)
    val sourceLine1 = generateSourceLine(stopId = stop1._2.stopID, timeStamp = startingArrivalTime)
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(10)
    val sourceLine2 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = startingArrivalTime + 5000)
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue should not be defined

    val sourceLine3 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = startingArrivalTime + 31000)
    f.sourceLineHandler.handle(sourceLine3).value.futureValue

    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue.value shouldBe 10
  }

  test("A Persisted record is stored in Redis DB") { f =>
    val timestamp1 = System.currentTimeMillis() + 60000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(9)
    val sourceLine1 = generateSourceLine(stopId = stop1._2.stopID, timeStamp = timestamp1)
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val timestamp2 = timestamp1 + 120000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(10)
    val sourceLine2 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = timestamp2)
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue.value shouldBe 10

   val recordFromRedis = f.redisDurationRecorder.getStopToStopTimes(BusRoute("25", "outbound"), 9, 10).futureValue
    recordFromRedis should have size 1
    recordFromRedis.head shouldBe ((timestamp2 - timestamp1) / 1000)
  }

  test("Multiple records for the same route (different vehicles) are stored in Redis DB") { f =>
    val timestamp1 = System.currentTimeMillis() + 60000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(9)
    val sourceLine1 = generateSourceLine(stopId = stop1._2.stopID, timeStamp = timestamp1, vehicleId = "VEHICLE1")
    f.sourceLineHandler.handle(sourceLine1).value.futureValue

    val timestamp2 = timestamp1 + 120000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(10)
    val sourceLine2 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = timestamp2, vehicleId = "VEHICLE1")
    f.sourceLineHandler.handle(sourceLine2).value.futureValue

    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue.value shouldBe 10

    val timestamp3 = System.currentTimeMillis() + 60000
    val sourceLine3 = generateSourceLine(stopId = stop1._2.stopID, timeStamp = timestamp3, vehicleId = "VEHICLE2")
    f.sourceLineHandler.handle(sourceLine3).value.futureValue

    val timestamp4 = timestamp3 + 30000
    val sourceLine4 = generateSourceLine(stopId = stop2._2.stopID, timeStamp = timestamp4, vehicleId = "VEHICLE2")
    f.sourceLineHandler.handle(sourceLine4).value.futureValue

    getLastIndexPersistedFromCache(sourceLine3, f.cache).futureValue.value shouldBe 10

    val recordFromRedis = f.redisDurationRecorder.getStopToStopTimes(BusRoute("25", "outbound"), 9, 10).futureValue
    recordFromRedis should have size 2
    recordFromRedis.head shouldBe ((timestamp4 - timestamp3) / 1000)
    recordFromRedis(1) shouldBe ((timestamp2 - timestamp1) / 1000)
  }

  def getArrivalTimestampFromCache(sourceLine: SourceLine, cache: ScalaCache[NoSerialization]): Future[Option[Long]] = {
    implicit val c: ScalaCache[NoSerialization] = cache
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val stopList = definitions.routeDefinitions.getOrElse(busRoute, throw new RuntimeException(s"Unable to find route $busRoute in definitions after validation passed"))
    val indexOfStop = stopList.find(_._2.stopID == sourceLine.stopID).map(_._1).getOrElse(throw new RuntimeException(s"Unable to find stopID ${sourceLine.stopID} in stop list for route $busRoute"))
    get[Long, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction, indexOfStop)
  }

  def getLastIndexPersistedFromCache(sourceLine: SourceLine, cache: ScalaCache[NoSerialization]): Future[Option[Int]] = {
    implicit val c: ScalaCache[NoSerialization] = cache
    get[Int, NoSerialization](sourceLine.vehicleID, sourceLine.route, sourceLine.direction)
  }


}