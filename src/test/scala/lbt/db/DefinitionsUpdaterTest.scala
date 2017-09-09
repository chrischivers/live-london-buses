package lbt.db

import lbt.ConfigLoader
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}
import org.scalatest.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._

class DefinitionsUpdaterTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(1 second)
  )

  case class FixtureParam(definitionsUpdater: BusRouteDefinitionsUpdater, definitionsTable: RouteDefinitionsTable)

  def withFixture(test: OneArgTest) = {
    val db = new PostgresDB(config.dBConfig)
    val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)

    val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)

    val testFixture = FixtureParam(updater, routeDefinitionsTable)

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
    f.definitionsUpdater.start(limitUpdateTo = Some(List(busRoute))).futureValue //Do update for Route 3, Outbound

    val stopList = f.definitionsTable.getStopSequenceFor(busRoute).futureValue
    stopList should have size 43
    stopList.head._1 shouldBe 0
    stopList.head._2.stopName should include("Trafalgar Sq")
    stopList.last._1 shouldBe 42
    stopList.last._2.stopName should include("Crystal Palace")
  }

  test("Routes persisted to DB should match those retrieved for all routes") { f =>

    val busRoute = BusRoute("3", "outbound")
    f.definitionsUpdater.start(limitUpdateTo = Some(List(busRoute))).futureValue //Do update for Route 3, Outbound

    val stopList = f.definitionsTable.getAllRouteDefinitions.futureValue
    stopList.keys should have size 1
    stopList.get(busRoute).value should have size 43
    stopList.get(busRoute).value.head._1 shouldBe 0
    stopList.get(busRoute).value.head._2.stopName should include("Trafalgar Sq")
    stopList.get(busRoute).value.last._1 shouldBe 42
    stopList.get(busRoute).value.last._2.stopName should include("Crystal Palace")
  }
}