package lbt.web

import akka.actor.ActorSystem
import cats.effect.IO
import com.github.andyglow.websocket.WebsocketClient
import com.typesafe.scalalogging.StrictLogging
import fs2.{Scheduler, Stream}
import lbt.{ConfigLoader, WebSocketsServer}
import lbt.common.Definitions
import lbt.db.caching.{RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, OptionValues, fixture}
import org.scalatest.Matchers._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Properties.envOrNone

class WebSocketServiceTest extends fixture.FunSuite with ScalaFutures with OptionValues with BeforeAndAfterAll with StrictLogging {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(1 second)
  )

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)


  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  case class FixtureParam(webSocketReceivedBuffer: ListBuffer[String], websocketClient: WebsocketClient[String])

  def withFixture(test: OneArgTest) = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisConfig = config.redisDBConfig.copy(dbIndex = 1)
    val redisSubscriberCache = new RedisSubscriberCache(redisConfig) // 1 = test, 0 = main
    val redisWsClientCache = new RedisWsClientCache(redisConfig)
    val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)
    val webSocketService: WebSocketService = new WebSocketService(webSocketClientHandler)

    var wsReceivedBuffer = new ListBuffer[String]()

    def addToWsResponseBuffer(msg: String) = wsReceivedBuffer += msg

    object TestWebSocketsServer extends StreamApp[IO] with Http4sDsl[IO] {

      logger.info(s"Starting up web socket service using port $port")

      def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, Nothing] =
        Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
          BlazeBuilder[IO]
            .bindHttp(port)
            .withWebSockets(true)
            .mountService(webSocketService.service(scheduler), "/ws")
            .serve
        }
    }

    Future(TestWebSocketsServer.main(Array.empty))
    Thread.sleep(5000)

    val websocketClient = WebsocketClient[String]("ws://localhost:8080/ws?uuid=1000") {
      case str => addToWsResponseBuffer(str)
    }

    val testFixture = FixtureParam(wsReceivedBuffer, websocketClient)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      websocketClient.shutdownSync()
    }
  }

  test("Websocket should stream data when opened") { f =>
    f.webSocketReceivedBuffer should have size 0
    f.websocketClient.open()
    Thread.sleep(2000)
    f.webSocketReceivedBuffer.size should be > 0

  }

}

