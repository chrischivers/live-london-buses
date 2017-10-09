package lbt.streaming

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import lbt.common.{Commons, Definitions}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.{ConfigLoader, SharedTestFeatures}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.util.Timeout
import lbt.streaming.Vehicle.StopIndex

import scala.concurrent.duration._

class VehicleTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll {

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

  test("Vehicle actor handles validated source lines") { f =>
    val actorSystem = ActorSystem()
    val stopArrivalRecord = generateStopArrivalRecord()
    val stopList = definitions.routeDefinitions(stopArrivalRecord.busRoute)
    val timestamp = System.currentTimeMillis() + 100000

    val vehicle = actorSystem.actorOf(Props(new Vehicle(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute, stopList)),
      name = Vehicle.generateActorId(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute))

    vehicle ! Handle(stopArrivalRecord, timestamp)

    val result = (vehicle ? GetArrivalTimesMap).futureValue.asInstanceOf[Map[StopIndex, Long]]
    result should have size 1
    result(stopArrivalRecord.stopIndex) shouldBe timestamp
  }

  test("Vehicle actor updates source line map when more recent times received") { f =>
    val actorSystem = ActorSystem()
    val stopArrivalRecord = generateStopArrivalRecord()
    val stopList = definitions.routeDefinitions(stopArrivalRecord.busRoute)
    val timestamp1 = System.currentTimeMillis() + 100000
    val timestamp2 = System.currentTimeMillis() + 50000

    val vehicle = actorSystem.actorOf(Props(new Vehicle(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute, stopList)),
      name = Vehicle.generateActorId(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute))

    vehicle ! Handle(stopArrivalRecord, timestamp1)
    vehicle ! Handle(stopArrivalRecord, timestamp2)

    val result = (vehicle ? GetArrivalTimesMap).futureValue.asInstanceOf[Map[StopIndex, Long]]
    result should have size 1
    result(stopArrivalRecord.stopIndex) shouldBe timestamp2
  }

  //TODO more here

}
