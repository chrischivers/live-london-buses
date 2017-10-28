package lbt.web

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import cats.Id
import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.common.Definitions
import lbt.db.caching._
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusPolyLine, BusRoute}
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.{ConfigLoader, LBTConfig, SharedTestFeatures}
import org.http4s.client.Client
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.{Method, Request, Uri}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class MapServiceTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll with EitherValues with StrictLogging with Eventually {

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  val config: LBTConfig = ConfigLoader.defaultConfig
  val portIncrementer = new AtomicInteger(8000)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(20 seconds),
    interval = scaled(1 second))

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound"), BusRoute("3", "inbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)


  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache, redisArrivalTimeLog: RedisArrivalTimeLog, httpClient: Client[IO], port: Int)

  def withFixture(test: OneArgTest) = {

    val port: Int = portIncrementer.incrementAndGet()

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisConfig = config.redisConfig.copy(dbIndex = 1)
    val redisSubscriberCache = new RedisSubscriberCache(redisConfig) // 1 = test, 0 = main
    val redisWsClientCache = new RedisWsClientCache(redisConfig, redisSubscriberCache)
    val redisArrivalTimeLog = new RedisArrivalTimeLog(redisConfig)
    val mapService = new MapService(config.mapServiceConfig, definitions, redisWsClientCache, redisSubscriberCache, redisArrivalTimeLog)

    val builder = BlazeBuilder[IO].bindHttp(port, "localhost").mountService(mapService.service, "/map").start
    builder.unsafeRunSync
    val httpClient = PooledHttp1Client[IO]()

    val testFixture = FixtureParam(redisWsClientCache, redisSubscriberCache, redisArrivalTimeLog, httpClient, port)
    try {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }

  test("Map is served from /map endpoint") { f =>
    val response = f.httpClient.expect[String](s"http://localhost:${f.port}/map")
    response.unsafeRunSync() should include("<title>Live London Buses</title>")
  }

  test("Assets are served from /map/assets endpoint") { f =>
    val response = f.httpClient.expect[String](s"http://localhost:${f.port}/map/assets/css/bootstrap.min.css")
    response.unsafeRunSync() should include("Bootstrap v3.3.7")
  }

  test("Snapshot without a UUID results in 404") { f =>
    val filteringParams = createFilteringParams()
    val request: IO[Request[IO]] = Request()
      .withMethod(Method.POST)
      .withUri(Uri.fromString(s"http://localhost:${f.port}/map/snapshot").right.get)
      .withBody(filteringParams.asJson.noSpaces)

    val response = f.httpClient.status(request).unsafeRunSync()
    response.code shouldBe 404
  }

  test("Snapshot is empty when no InProgress data exists") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result shouldBe empty
  }

  test("Snapshot is returned when InProgress data exists") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() - 10000)
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result should have size 1
    result.head.copy(startingTime = 0, movementInstructionsToNext = None, startingLatLng = busPositionData.startingLatLng) shouldBe busPositionData.copy(startingTime = 0, movementInstructionsToNext = None)
  }

  test("Snapshot is empty when InProgress data exists but not route in filtering params") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(busRoute = BusRoute("5", "outbound"), arrivalTimeStamp = System.currentTimeMillis() - 10000)
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result shouldBe empty
  }

  test("Snapshot is empty when InProgress data exists but starts after current time") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() + 5000)
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result shouldBe empty
  }

  test("Snapshot is empty when InProgress data exists but ends before current time") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() - 10000, arrivalTimeAtNextStop = Some(System.currentTimeMillis() - 1))
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result shouldBe empty
  }

  test("Snapshot is empty when InProgress data doesn't have a nextstoparrival time") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(arrivalTimeAtNextStop = None)
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result shouldBe empty
  }

  test("Duplicate bus data for same vehicle is disregarded. Only most imminent is obtained") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData1 = createBusPositionData(vehicleId = "VEHICLE1", arrivalTimeStamp = System.currentTimeMillis() - 10000, arrivalTimeAtNextStop = Some(System.currentTimeMillis() + 60000))
    val busPositionData2 = createBusPositionData(vehicleId = "VEHICLE1", arrivalTimeStamp = System.currentTimeMillis() - 10000, arrivalTimeAtNextStop = Some(System.currentTimeMillis() + 120000))
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData1).futureValue
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData2).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result should have size 1
    result.head.copy(startingTime = 0, movementInstructionsToNext = None, startingLatLng = busPositionData2.startingLatLng) shouldBe busPositionData2.copy(startingTime = 0, movementInstructionsToNext = None)
  }

  test("Start time for InProgress data is adjusted to reflect current time") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() - 60000, arrivalTimeAtNextStop = Some(System.currentTimeMillis() + 60000))
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result should have size 1
    result.head.startingTime should (be < System.currentTimeMillis() + 1000 and be > System.currentTimeMillis() - 1000)
  }

  test("Movement Instruction List for InProgress data is adjusted in proportion to current time remaining") { f =>
    val filteringParams = createFilteringParams()
    val uuid = UUID.randomUUID().toString
    val busPositionData = createBusPositionData(
      movementInstructionsOpt = Some(BusPolyLine("}uhyH~mWjBYN??E@G@EHKHAB?FcBx@wOFkACg@Ig@KSEMAQ@QT[Ji@Kq@s@aB").toMovementInstructions),
      arrivalTimeStamp = System.currentTimeMillis() - 60000,
      arrivalTimeAtNextStop = Some(System.currentTimeMillis() + 60000))
    f.redisWsClientCache.storeVehicleActivityInProgress(busPositionData).futureValue
    busPositionData.movementInstructionsToNext.value should have size 21
    val result = parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, filteringParams)).unsafeRunSync()).value
    result should have size 1
    result.head.movementInstructionsToNext.value.size shouldBe 11
    result.head.movementInstructionsToNext.value.map(_.copy(proportion = 0)) shouldBe
      busPositionData.movementInstructionsToNext.value.takeRight(11).map(_.copy(proportion = 0)) //disregard proportion as adjusted
    val sumOfProportions = result.head.movementInstructionsToNext.value.foldLeft(0.0)((acc, ins) => acc + ins.proportion)
    val roundedSum = BigDecimal(sumOfProportions).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    roundedSum shouldBe 1.0
  }

    test("Snapshot request updates filtering params") { f =>

      val uuid = UUID.randomUUID().toString
      val params = createFilteringParams()
      f.redisSubscriberCache.subscribe(uuid, None).futureValue
      f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe List(uuid)
      f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue should not be defined

     parseWebsocketCacheResult(f.httpClient.fetchAs[String](generateSnapshotRequest(f.port, uuid, params)).unsafeRunSync()).value

      eventually {
        val paramsFromCache = f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue
        paramsFromCache shouldBe defined
          paramsFromCache.value shouldBe params
      }

    }

  test("NextStops request returns all records for vehicleId and Route") { f =>

    val vehicleId = "VEHICLE1"
    val busRoute = BusRoute("25", "outbound")

    val stopIndex1 = 5
    val arrivalTimeStamp1 = System.currentTimeMillis() + 200000
    val arrivalRecord1 = generateStopArrivalRecord(vehicleId, busRoute, stopIndex = stopIndex1)

    val stopIndex2 = 6
    val arrivalTimeStamp2 = System.currentTimeMillis() + 300000
    val arrivalRecord2 = generateStopArrivalRecord(vehicleId, busRoute, stopIndex = stopIndex2)

    f.redisArrivalTimeLog.addArrivalRecord(arrivalTimeStamp1, arrivalRecord1).futureValue
    f.redisArrivalTimeLog.addArrivalRecord(arrivalTimeStamp2, arrivalRecord2).futureValue

    val result = parseNextStopsJson(generateNextStopsRequest(f.httpClient, f.port, vehicleId, busRoute).unsafeRunSync()).value
    result should have size 2
    result.head.vehicleId shouldBe vehicleId
    result.head.busRoute shouldBe busRoute
    result.head.stopIndex shouldBe arrivalRecord1.stopIndex
    result.head.predictedArrival shouldBe arrivalTimeStamp1
    result.head.busStop shouldBe definitions.routeDefinitions(busRoute).find(_._1 == stopIndex1).value._2

    result(1).vehicleId shouldBe vehicleId
    result(1).busRoute shouldBe busRoute
    result(1).stopIndex shouldBe arrivalRecord2.stopIndex
    result(1).predictedArrival shouldBe arrivalTimeStamp2
    result(1).busStop shouldBe definitions.routeDefinitions(busRoute).find(_._1 == stopIndex2).value._2

  }

  def generateSnapshotRequest(port: Int, uuid: String, filteringParams: FilteringParams): IO[Request[IO]] = {
    Request()
      .withMethod(Method.POST)
      .withUri(Uri.fromString(s"http://localhost:$port/map/snapshot?uuid=$uuid").right.get)
      .withBody(filteringParams.asJson.noSpaces)
  }

  def generateNextStopsRequest(httpClient: Client[IO], port: Int, vehicleId: String, busRoute: BusRoute): IO[String] = {

    httpClient.expect[String](Uri.fromString(s"http://localhost:$port/map/nextstops" +
      s"?vehicleId=$vehicleId" +
      s"&routeId=${busRoute.id}" +
      s"&direction=${busRoute.direction}").right.get)
  }
}
