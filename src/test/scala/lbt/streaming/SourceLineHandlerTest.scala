package lbt.streaming

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.util.Timeout
import lbt.common.{Commons, Definitions}
import lbt.db.caching.{RedisArrivalTimeLog, RedisVehicleArrivalTimeLog}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}
import org.scalatest.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SourceLineHandlerTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1 minute),
    interval = scaled(500 millis)
  )

  implicit val akkaTimeout = Timeout(3, TimeUnit.SECONDS)

  //These are set outside the fixture so that definitions db does not need to be repopulated before each test
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)

  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(definitions: Definitions, sourceLineHandler: SourceLineHandler, redisArrivalTimeLog: RedisArrivalTimeLog, redisVehicleArrivalTimeLog: RedisVehicleArrivalTimeLog)

  def withFixture(test: OneArgTest) = {
    implicit val actorSystem = ActorSystem()
    val definitions = new Definitions(routeDefinitionsTable)
    val redisConfig = config.redisConfig.copy(dbIndex = 1)

    val redisArrivalTimeLog = new RedisArrivalTimeLog(redisConfig) // 1 = test, 0 = main
    val redisVehicleArrivalTimeLog = new RedisVehicleArrivalTimeLog(redisConfig, config.streamingConfig)
    val sourceLineHandler = new SourceLineHandler(redisArrivalTimeLog, redisVehicleArrivalTimeLog, definitions, config.streamingConfig)
    val testFixture = FixtureParam(definitions, sourceLineHandler, redisArrivalTimeLog, redisVehicleArrivalTimeLog)

    try {
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    } finally {
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }


  test("Source Line Handler sends message to arrival time cache and vehicle arrival time cache") { f =>

    val sourceLine = generateSourceLine()
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val indexOfStop = f.definitions.routeDefinitions(busRoute).find(_._2.stopID == sourceLine.stopID).value._1
    f.sourceLineHandler.handle(sourceLine).futureValue

    val resultFromArrivalTimeCache = f.redisArrivalTimeLog.getAndDropArrivalRecords(sourceLine.arrivalTimeStamp + 1).futureValue
    resultFromArrivalTimeCache should have size 1
    resultFromArrivalTimeCache.head._1 shouldBe StopArrivalRecord(sourceLine.vehicleId, busRoute, indexOfStop, lastStop = false)
    resultFromArrivalTimeCache.head._2 shouldBe sourceLine.arrivalTimeStamp

    val resultFromVehicleArrivalTimeCache = f.redisVehicleArrivalTimeLog.getArrivalTimes(sourceLine.vehicleId, busRoute).futureValue
    resultFromVehicleArrivalTimeCache should have size 1
    resultFromVehicleArrivalTimeCache.head.stopIndex shouldBe indexOfStop
    resultFromVehicleArrivalTimeCache.head.arrivalTime shouldBe sourceLine.arrivalTimeStamp
  }

  test("Source Line Handler sends message to arrival time cache and vehicle arrival and set lastStop to true") { f =>

    val sourceLine = generateSourceLine(stopId = "490007657S")
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val indexOfStop = f.definitions.routeDefinitions(busRoute).find(_._2.stopID == sourceLine.stopID).value._1
    f.sourceLineHandler.handle(sourceLine).futureValue

    val resultFromArrivalTimeCache = f.redisArrivalTimeLog.getAndDropArrivalRecords(sourceLine.arrivalTimeStamp + 1).futureValue
    resultFromArrivalTimeCache should have size 1
    resultFromArrivalTimeCache.head._1 shouldBe StopArrivalRecord(sourceLine.vehicleId, busRoute, indexOfStop, lastStop = true)
    resultFromArrivalTimeCache.head._2 shouldBe sourceLine.arrivalTimeStamp

    val resultFromVehicleArrivalTimeCache = f.redisVehicleArrivalTimeLog.getArrivalTimes(sourceLine.vehicleId, busRoute).futureValue
    resultFromVehicleArrivalTimeCache should have size 1
    resultFromVehicleArrivalTimeCache.head.stopIndex shouldBe indexOfStop
    resultFromVehicleArrivalTimeCache.head.arrivalTime shouldBe sourceLine.arrivalTimeStamp
  }

  test("Source Line for same route/vehicle/index is updated not appended") { f =>

    val sourceLine1 = generateSourceLine(timeStamp = System.currentTimeMillis() + 100000)
    val sourceLine2 = generateSourceLine(timeStamp = System.currentTimeMillis() + 150000)
    val busRoute = BusRoute(sourceLine1.route, Commons.toDirection(sourceLine1.direction))
    val indexOfStop = f.definitions.routeDefinitions(busRoute).find(_._2.stopID == sourceLine1.stopID).value._1
    f.sourceLineHandler.handle(sourceLine1).futureValue
    f.sourceLineHandler.handle(sourceLine2).futureValue

    val resultFromArrivalTimeCache = f.redisArrivalTimeLog.getAndDropArrivalRecords(sourceLine2.arrivalTimeStamp + 1).futureValue
    resultFromArrivalTimeCache should have size 1
    resultFromArrivalTimeCache.head._1 shouldBe StopArrivalRecord(sourceLine2.vehicleId, busRoute, indexOfStop, lastStop = false)
    resultFromArrivalTimeCache.head._2 shouldBe sourceLine2.arrivalTimeStamp

    val resultFromVehicleArrivalTimeCache = f.redisVehicleArrivalTimeLog.getArrivalTimes(sourceLine1.vehicleId, busRoute).futureValue
    resultFromVehicleArrivalTimeCache should have size 1
    resultFromVehicleArrivalTimeCache.head.stopIndex shouldBe indexOfStop
    resultFromVehicleArrivalTimeCache.head.arrivalTime shouldBe sourceLine2.arrivalTimeStamp
  }
}
