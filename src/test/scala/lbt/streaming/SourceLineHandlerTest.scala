package lbt.streaming

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import lbt.common.{Commons, Definitions}
import lbt.db.caching.RedisArrivalTimeLog
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.streaming.Vehicle.StopIndex
import lbt.streaming.VehicleCoordinator.{LastUpdated, VehicleActorID}
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

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

  case class FixtureParam(definitions: Definitions, sourceLineHandler: SourceLineHandler, vehicleCoordinator: ActorRef, redisArrivalTimeLog: RedisArrivalTimeLog)

  def withFixture(test: OneArgTest) = {
      implicit val actorSystem = ActorSystem()
    val definitions = new Definitions(routeDefinitionsTable)
      val testVehicleCoordinator = actorSystem.actorOf(Props(new VehicleCoordinator(definitions)))


      val redisArrivalTimeLog = new RedisArrivalTimeLog(config.redisDBConfig.copy(dbIndex = 1)) // 1 = test, 0 = main
      val sourceLineHandler = new SourceLineHandler(redisArrivalTimeLog, definitions) {
        override val vehicleCoordinator = testVehicleCoordinator
      }
      val testFixture = FixtureParam(definitions, sourceLineHandler, testVehicleCoordinator, redisArrivalTimeLog)

    try {
      redisArrivalTimeLog.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    } finally {
      redisArrivalTimeLog.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }

  test("Source Line Handler sends source line to vehicle coordinator and redis arrival time log") { f =>

    val sourceLine = generateSourceLine()
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val indexOfStop = f.definitions.routeDefinitions(busRoute).find(_._2.stopID == sourceLine.stopID).value._1
    f.sourceLineHandler.handle(sourceLine).futureValue


    val resultFromCoordinator = (f.vehicleCoordinator ? GetLiveVehicleList).futureValue.asInstanceOf[Map[VehicleActorID, (ActorRef, LastUpdated)]]
    resultFromCoordinator should have size 1
    resultFromCoordinator.head._1 shouldBe Vehicle.generateActorId(sourceLine.vehicleId, busRoute)

    val resultFromRedis = f.redisArrivalTimeLog.getArrivalRecords(sourceLine.arrivalTimeStamp + 1).futureValue
    resultFromRedis should have size 1
    resultFromRedis.head._1 shouldBe StopArrivalRecord(sourceLine.vehicleId, busRoute, indexOfStop)
    resultFromRedis.head._2 shouldBe sourceLine.arrivalTimeStamp
  }
}
