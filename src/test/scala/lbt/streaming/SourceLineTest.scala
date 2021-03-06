package lbt.streaming

import lbt.{ConfigLoader, SharedTestFeatures}
import lbt.common.Definitions
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SourceLineTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll {


  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  //These are set outside the fixture so that definitions db does not need to be repopulated before each test
  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound")))).futureValue

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

  test("Raw source lines can be converted to SourceLine types") { f =>
    val time = System.currentTimeMillis() + 30000
    val line = s"""[1,"490007497E","25",1,"Ilford","BJ11DUV",$time]"""

    val sourceLine = SourceLine.fromRawLine(line).value

    sourceLine.stopID shouldBe "490007497E"
    sourceLine.route shouldBe "25"
    sourceLine.direction shouldBe 1
    sourceLine.destinationText shouldBe "Ilford"
    sourceLine.vehicleId shouldBe "BJ11DUV"
    sourceLine.arrivalTimeStamp shouldBe time
  }

  test("Source Line validation passes if all criteria met") { f =>
    val sourceLinePast = generateSourceLine()
    SourceLine.validate(sourceLinePast, f.definitions) shouldBe true
  }

  test("Source Line validation fails if time is in the past") { f =>
    val sourceLinePast = generateSourceLine(timeStamp = System.currentTimeMillis() - 1000)
    SourceLine.validate(sourceLinePast, f.definitions) shouldBe false
  }

//  test("Source Line validation fails if time is in too far in the future") { f =>
//    val sourceLinePast = generateSourceLine(timeStamp = System.currentTimeMillis() + 1000000)
//    SourceLine.validate(sourceLinePast, f.definitions) shouldBe false
//  }

  test("Source Line validation fails if route is not in the definitions ") { f =>
    val sourceLineInvalidRoute = generateSourceLine(route = "999", direction = 1)
    SourceLine.validate(sourceLineInvalidRoute, f.definitions) shouldBe false
  }

  test("Source Line validation fails if stop is not in definitions") { f =>
    val sourceLineInvalidStop = generateSourceLine(stopId = "49000INVALID")
    SourceLine.validate(sourceLineInvalidStop, f.definitions) shouldBe false
  }

}
