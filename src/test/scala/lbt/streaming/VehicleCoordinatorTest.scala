package lbt.streaming

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import lbt.common.{Commons, Definitions}
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

class VehicleCoordinatorTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll {


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

  case class FixtureParam(definitions: Definitions)

  def withFixture(test: OneArgTest) = {
    val definitions = new Definitions(routeDefinitionsTable)
    val testFixture = FixtureParam(definitions)
    withFixture(test.toNoArgTest(testFixture))
  }

  test("Vehicle coordinator creates a new vehicle for new registration/route and sends along source line") { f =>

    implicit val actorSystem = ActorSystem()
    val vehicleCoordinator = actorSystem.actorOf(Props(new VehicleCoordinator(f.definitions)))
    val stopArrivalRecord = generateStopArrivalRecord()
    val timeStamp = System.currentTimeMillis() + 60000
    vehicleCoordinator ! Handle(stopArrivalRecord, timeStamp)

    val resultFromCoordinator = (vehicleCoordinator ? GetLiveVehicleList).futureValue.asInstanceOf[Map[VehicleActorID, (ActorRef, LastUpdated)]]
    resultFromCoordinator should have size 1
    val actorId = Vehicle.generateActorId(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute)
    resultFromCoordinator(actorId)._2 shouldBe < (System.currentTimeMillis())
    resultFromCoordinator(actorId)._2 shouldBe > (System.currentTimeMillis() - 10000)

    val resultFromVehicle = (resultFromCoordinator(actorId)._1 ? GetArrivalTimesMap).futureValue.asInstanceOf[Map[StopIndex, Long]]
    resultFromVehicle should have size 1
    resultFromVehicle(stopArrivalRecord.stopIndex) shouldBe timeStamp

  }

  test("Vehicle coordinator does not create duplicate vehicle actors for known registration/route") { f =>

    implicit val actorSystem = ActorSystem()
    val vehicleCoordinator = actorSystem.actorOf(Props(new VehicleCoordinator(f.definitions)))
    val busRoute = BusRoute("25", "outbound")

    val stopArrivalRecord1 = generateStopArrivalRecord(stopIndex = f.definitions.routeDefinitions(busRoute)(5)._1)
    val timestamp1 = System.currentTimeMillis() + 100000
    val stopArrivalRecord2 = generateStopArrivalRecord(stopIndex = f.definitions.routeDefinitions(busRoute)(6)._1)
    val timestamp2 = System.currentTimeMillis() + 200000

    vehicleCoordinator ! Handle(stopArrivalRecord1, timestamp1)
    vehicleCoordinator ! Handle(stopArrivalRecord2, timestamp2)

    val resultFromCoordinator = (vehicleCoordinator ? GetLiveVehicleList).futureValue.asInstanceOf[Map[VehicleActorID, (ActorRef, LastUpdated)]]
    resultFromCoordinator should have size 1
    val actorId = Vehicle.generateActorId(stopArrivalRecord1.vehicleId, busRoute)

    val resultFromVehicle = (resultFromCoordinator(actorId)._1 ? GetArrivalTimesMap).futureValue.asInstanceOf[Map[StopIndex, Long]]
    resultFromVehicle should have size 2

  }

}
