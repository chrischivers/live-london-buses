package lbt.web

import akka.actor.ActorSystem
import cats.effect.IO
import lbt.ConfigLoader
import lbt.common.Definitions
import lbt.db.caching.RedisDurationRecorder
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.client.{Client, UnexpectedStatus}
import org.http4s.server.blaze.BlazeBuilder
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Properties.envOrNone

class LbtServletTest extends fixture.FunSuite with ScalaFutures with OptionValues with BeforeAndAfterAll {

  val config = ConfigLoader.defaultConfig
  val httpTimeout = 30.seconds

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(1 second)
  )

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip: String = "0.0.0.0"

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)

  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(httpClient: Client[IO])

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisClient = new RedisDurationRecorder(config.redisDBConfig.copy(dbIndex = 1)) // 1 = test, 0 = main
    val lbtServlet = new LbtServlet(redisClient, definitions)
    val httpClient = PooledHttp1Client[IO]()

    println(s"Starting up servlet using port $port bound to ip $ip")
    val server = BlazeBuilder[IO]
      .bindHttp(port, ip)
      .withIdleTimeout(3.minutes)
      .mountService(lbtServlet.service)
      .start
      .unsafeRunSync()

    val testFixture = FixtureParam(httpClient)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
     server.shutdownNow()
    }
  }

  test("Endpoint returns status 200 for known route") { f =>
    f.httpClient.expect[String]("http://localhost:8080/lbt/25/outbound").unsafeRunTimed(httpTimeout)
  }

  test("Endpoint returns status 404 for unknown route") { f =>
    assertThrows[UnexpectedStatus] {
      f.httpClient.expect[String]("http://localhost:8080/lbt/999/outbound").unsafeRunTimed(httpTimeout)
    }
  }
}