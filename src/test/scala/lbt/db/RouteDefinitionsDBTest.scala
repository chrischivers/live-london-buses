package lbt.db

import lbt.ConfigLoader
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusPolyLine, BusRoute, BusStop, LatLng}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RouteDefinitionsDBTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(definitionsTable: RouteDefinitionsTable)

  def withFixture(test: OneArgTest) = {
    val db = new PostgresDB(config.postgresDbConfig)
    val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)

    val testFixture = FixtureParam(routeDefinitionsTable)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      routeDefinitionsTable.dropTable.futureValue
      db.disconnect.futureValue
    }
  }

  test("Routes persisted to DB should match those retrieved for a given route") { f =>

    val busRoute = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", LatLng(51, 41))
    val busStop2 = BusStop("Id2", "Name2", LatLng(52, 42))
    val busStop3 = BusStop("Id3", "Name3", LatLng(53, 43))

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1, busStop2, busStop3).zipWithIndex).futureValue

    val stopList = f.definitionsTable.getStopSequenceFor(busRoute).futureValue
    stopList should have size 3
    stopList.head shouldBe ((0, busStop1, None))
    stopList(1) shouldBe ((1, busStop2, None))
    stopList(2) shouldBe ((2, busStop3, None))
  }

  test("Error is thrown if attempt is made to insert route that already exists") { f =>

    val busRoute = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", LatLng(51, 41))

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1).zipWithIndex).futureValue

    assertThrows[RuntimeException] {
      f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1).zipWithIndex).futureValue
    }
  }

  test("Routes persisted to DB should match those retrieved when all definitions are retrieved") { f =>

    val busRouteA = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", LatLng(51, 41))
    val busStop2 = BusStop("Id2", "Name2", LatLng(52, 42))
    val busStop3 = BusStop("Id3", "Name3", LatLng(53, 43))

    val busRouteB = BusRoute("4", "inbound")
    val busStop4 = BusStop("Id4", "Name4", LatLng(54, 44))
    val busStop5 = BusStop("Id5", "Name5", LatLng(55, 45))

    f.definitionsTable.insertRouteDefinitions(busRouteA, List(busStop1, busStop2, busStop3).zipWithIndex).futureValue
    f.definitionsTable.insertRouteDefinitions(busRouteB, List(busStop4, busStop5).zipWithIndex).futureValue

    val stopList = f.definitionsTable.getAllRouteDefinitions.futureValue
    stopList.keys should have size 2

    val busRouteAResults = stopList.get(busRouteA).value
    busRouteAResults should have size 3
    busRouteAResults.head shouldBe((0, busStop1, None))
    busRouteAResults(1) shouldBe((1, busStop2, None))
    busRouteAResults(2) shouldBe((2, busStop3, None))

    val busRouteBResults = stopList.get(busRouteB).value
    busRouteBResults should have size 2
    busRouteBResults.head shouldBe((0, busStop4, None))
    busRouteBResults(1) shouldBe((1, busStop5, None))

  }

  test("Polylines can be updated to existing records in the DB") { f =>

    val busRoute = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", LatLng(51.5076, -0.129804))
    val busStop2 = BusStop("Id2", "Name2", LatLng(51.5065, -0.127142))
    val polyLine =  BusPolyLine("TESTPOLYLINE")

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1, busStop2).zipWithIndex).futureValue

    f.definitionsTable.updatePolyLine(busRoute, 0, polyLine).futureValue

    val returnedRecords = f.definitionsTable.getStopSequenceFor(busRoute).futureValue
    returnedRecords should have size 2
    returnedRecords.head._3 shouldBe Some(polyLine)
    returnedRecords(1)._3 shouldBe None

  }

  test("LatLng values persisted to the DB should be retrieved with matching precision") { f =>

    val busRoute = BusRoute("3", "outbound")
    val latLng = LatLng(51.5076, -0.129804)
    val busStop = BusStop("Id1", "Name1", latLng)

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop).zipWithIndex).futureValue

    val returnedRecords = f.definitionsTable.getStopSequenceFor(busRoute).futureValue
    returnedRecords should have size 1
    returnedRecords.head._2.latLng shouldBe latLng

  }
}