package lbt.streaming

import lbt.common.Commons
import lbt.db.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.{ConfigLoader, Definitions, LBTConfig}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalacache.guava.GuavaCache
import scalacache.{NoSerialization, ScalaCache, _}

class SourceLineHandlerTest extends fixture.FunSuite with ScalaFutures with OptionValues with BeforeAndAfterAll {

  val config: LBTConfig = ConfigLoader.defaultConfig
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

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

  case class FixtureParam(sourceLineHandler: SourceLineHandler, cache: ScalaCache[NoSerialization])

  def withFixture(test: OneArgTest) = {
    val cache = ScalaCache(GuavaCache())
    val sourceLineHandler = new SourceLineHandler(definitions)(cache, ec)
    val testFixture = FixtureParam(sourceLineHandler, cache)

    withFixture(test.toNoArgTest(testFixture))
  }

  test("Source Line handled is persisted to cache") { f =>
    val timestamp = System.currentTimeMillis() + 10000
    val sourceLine = generateSourceLine(timeStamp = timestamp)
    f.sourceLineHandler.handle(sourceLine).value.futureValue
    val timeStampFromCache = getArrivalTimestampFromCache(sourceLine, f.cache).futureValue.value
    timeStampFromCache shouldBe timestamp
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

  test("Cache is updated with last index persisted and persisted record removed") { f =>
    val timestamp1 = System.currentTimeMillis() + 10000
    val stop1 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(4)
    val sourceLine1 = generateSourceLine(timeStamp = timestamp1, stopId = stop1._2.stopID)

    f.sourceLineHandler.handle(sourceLine1).value.futureValue
    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue.value shouldBe timestamp1
    getLastIndexPersistedFromCache(sourceLine1, f.cache).futureValue should not be defined

    val timestamp2 = System.currentTimeMillis() + 5000
    val stop2 = definitions.routeDefinitions.get(BusRoute("25", "outbound")).value(5)
    val sourceLine2 = generateSourceLine(timeStamp = timestamp2, stopId = stop2._2.stopID)

    f.sourceLineHandler.handle(sourceLine2).value.futureValue
    getLastIndexPersistedFromCache(sourceLine2, f.cache).futureValue.value shouldBe 5

    getArrivalTimestampFromCache(sourceLine1, f.cache).futureValue should not be defined
    getArrivalTimestampFromCache(sourceLine2, f.cache).futureValue.value shouldBe timestamp2
  }





  def generateSourceLine(
                          route: String = "25",
                          direction: Int = 1,
                          stopId: String = "490007497E",
                          destination: String = "Ilford",
                          vehicleId: String = "BJ11DUV",
                          timeStamp: Long = System.currentTimeMillis() + 30000) = {
    SourceLine(route, direction, stopId, destination, vehicleId, timeStamp)
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

