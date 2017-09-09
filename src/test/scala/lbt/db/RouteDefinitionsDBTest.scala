package lbt.db

import lbt.ConfigLoader
import lbt.models.{BusRoute, BusStop}
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
    val db = new PostgresDB(config.dBConfig)
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
    val busStop1 = BusStop("Id1", "Name1", 51, 41)
    val busStop2 = BusStop("Id2", "Name2", 52, 42)
    val busStop3 = BusStop("Id3", "Name3", 53, 43)

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1, busStop2, busStop3).zipWithIndex).futureValue

    val stopList = f.definitionsTable.getStopSequenceFor(busRoute).futureValue
    stopList should have size 3
    stopList(0) shouldBe ((0, busStop1))
    stopList(1) shouldBe ((1, busStop2))
    stopList(2) shouldBe ((2, busStop3))
  }

  test("Error is thrown if attempt is made to insert route that already exists") { f =>

    val busRoute = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", 51, 41)

    f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1).zipWithIndex).futureValue

    assertThrows[RuntimeException] {
      f.definitionsTable.insertRouteDefinitions(busRoute, List(busStop1).zipWithIndex).futureValue
    }
  }

  test("Routes persisted to DB should match those retrieved when all definitions are retrieved") { f =>

    val busRouteA = BusRoute("3", "outbound")
    val busStop1 = BusStop("Id1", "Name1", 51, 41)
    val busStop2 = BusStop("Id2", "Name2", 52, 42)
    val busStop3 = BusStop("Id3", "Name3", 53, 43)

    val busRouteB = BusRoute("4", "inbound")
    val busStop4 = BusStop("Id4", "Name4", 54, 44)
    val busStop5 = BusStop("Id5", "Name5", 55, 45)

    f.definitionsTable.insertRouteDefinitions(busRouteA, List(busStop1, busStop2, busStop3).zipWithIndex).futureValue
    f.definitionsTable.insertRouteDefinitions(busRouteB, List(busStop4, busStop5).zipWithIndex).futureValue

    val stopList = f.definitionsTable.getAllRouteDefinitions.futureValue
    stopList.keys should have size 2

    val busRouteAResults = stopList.get(busRouteA).value
    busRouteAResults should have size 3
    busRouteAResults(0) shouldBe((0, busStop1))
    busRouteAResults(1) shouldBe((1, busStop2))
    busRouteAResults(2) shouldBe((2, busStop3))

    val busRouteBResults = stopList.get(busRouteB).value
    busRouteBResults should have size 2
    busRouteBResults(0) shouldBe((0, busStop4))
    busRouteBResults(1) shouldBe((1, busStop5))

  }
}