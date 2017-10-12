package lbt.web

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem, Props}
import cats.effect.IO
import cats.implicits._
import com.github.andyglow.websocket.WebsocketClient
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream._
import fs2.{Scheduler, Stream}
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException
import lbt.common.Definitions
import lbt.db.caching._
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.BusRoute
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.streaming.{CacheReadCommand, CacheReader, SourceLineHandler}
import lbt.{ConfigLoader, LBTConfig, SharedTestFeatures}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


class WebSocketServiceTest extends fixture.FunSuite with SharedTestFeatures with ScalaFutures with OptionValues with BeforeAndAfterAll with EitherValues with StrictLogging with Eventually {

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  val config: LBTConfig = ConfigLoader.defaultConfig
  val portIncrementer = new AtomicInteger(8000)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(20 seconds),
    interval = scaled(1 second))

  val db = new PostgresDB(config.postgresDbConfig)
  val routeDefinitionsTable = new RouteDefinitionsTable(db, RouteDefinitionSchema(tableName = "lbttest"), createNewTable = true)
  val updater = new BusRouteDefinitionsUpdater(config.definitionsConfig, routeDefinitionsTable)
  updater.start(limitUpdateTo = Some(List(BusRoute("25", "outbound"), BusRoute("3", "inbound")))).futureValue
  val definitions = new Definitions(routeDefinitionsTable)


  override protected def afterAll(): Unit = {
    routeDefinitionsTable.dropTable.futureValue
    db.disconnect.futureValue
  }

  def setUpTestWebSocketClient(uuid: String, port: Int): (WebsocketClient[String], ListBuffer[String]) = {
    var wsReceivedBuffer = new ListBuffer[String]()
    val websocketClient = WebsocketClient[String](s"ws://localhost:$port/ws?uuid=$uuid") {
      case msg => wsReceivedBuffer += msg
    }
    (websocketClient, wsReceivedBuffer)
  }

  case class FixtureParam(redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache, sourceLineHandler: SourceLineHandler, cacheReader: ActorRef, wsPort: Int)

  def withFixture(test: OneArgTest) = {

    val wsPort: Int = portIncrementer.incrementAndGet()

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisConfig = config.redisConfig.copy(dbIndex = 1)
    val redisSubscriberCache = new RedisSubscriberCache(redisConfig) // 1 = test, 0 = main
    val redisWsClientCache = new RedisWsClientCache(redisConfig, redisSubscriberCache)
    val redisArrivalTimeLog = new RedisArrivalTimeLog(redisConfig)
    val redisVehicleArrivalTimeLog = new RedisVehicleArrivalTimeLog(redisConfig, config.streamingConfig)

    val cacheReader = actorSystem.actorOf(Props(new CacheReader(redisArrivalTimeLog, redisVehicleArrivalTimeLog, redisSubscriberCache, redisWsClientCache, definitions)))

    val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)
    val webSocketService: WebSocketService = new WebSocketService(webSocketClientHandler, config.websocketConfig)

    val sourceLineHandler = new SourceLineHandler(redisArrivalTimeLog, redisVehicleArrivalTimeLog,definitions, config.streamingConfig)(ec)


    object TestWebSocketsServer extends StreamApp[IO] with Http4sDsl[IO] {
      logger.info(s"Starting up web socket service using port $wsPort")

      def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, Nothing] =
        Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
          BlazeBuilder[IO]
            .bindHttp(wsPort)
            .withWebSockets(true)
            .mountService(webSocketService.service(scheduler), "/ws")
            .serve
        }
    }

    Future(TestWebSocketsServer.main(Array.empty))
    Thread.sleep(2000)

    val testFixture = FixtureParam(redisWsClientCache, redisSubscriberCache, sourceLineHandler, cacheReader, wsPort)
    try {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      redisArrivalTimeLog.flushDB.futureValue
      redisVehicleArrivalTimeLog.flushDB.futureValue
      actorSystem.terminate().futureValue
    }
  }

  test("Websocket should stream data when opened, returning an empty response if not date has been stored for that UUID") { f =>

    val uuid = UUID.randomUUID().toString
    val (websocketClient, receivedBuffer) = setUpTestWebSocketClient(uuid, f.wsPort)
    f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe List.empty
    receivedBuffer should have size 0

    websocketClient.open()
    f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe List(uuid)

    f.redisWsClientCache.storeVehicleActivity("ANOTHER_UUID", createBusPositionData()).futureValue

    eventually {
      receivedBuffer.size should be > 0
      parsePacketsReceived(receivedBuffer) should have size 0
    }
    websocketClient.shutdownSync()
  }

  test("Websocket returns data if user uuid has been subscribed, with earliest timestamps first") { f =>

    val uuid = UUID.randomUUID().toString
    val (websocketClient, packagesReceivedBuffer) = setUpTestWebSocketClient(uuid, f.wsPort)

    packagesReceivedBuffer should have size 0
    websocketClient.open()

    val posData1 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis() - 10000)
    val posData2 = createBusPositionData(arrivalTimeStamp = System.currentTimeMillis())

    f.redisWsClientCache.storeVehicleActivity(uuid, posData1).futureValue
    f.redisWsClientCache.storeVehicleActivity(uuid, posData2).futureValue

    eventually {
      val messagesReceived = parsePacketsReceived(packagesReceivedBuffer)
      messagesReceived should have size 2
      messagesReceived.head shouldBe posData1
      messagesReceived(1) shouldBe posData2
    }
    websocketClient.shutdownSync()
  }

  test("Client is able to send filtering parameters, which are updated in the cache") { f =>

    val uuid = UUID.randomUUID().toString
    val (websocketClient, _) = setUpTestWebSocketClient(uuid, f.wsPort)
    val params = createFilteringParams()

    val openedSocket = websocketClient.open()
    f.redisSubscriberCache.getListOfSubscribers.futureValue shouldBe List(uuid)
    f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue should not be defined

    openedSocket ! createJsonStringFromFilteringParams(params)

    eventually {
      val paramsFromCache = f.redisSubscriberCache.getParamsForSubscriber(uuid).futureValue
      paramsFromCache shouldBe defined
        paramsFromCache.value shouldBe params
    }

    websocketClient.shutdownSync()
  }

  test("Client receives data for routes they are subscribed to") { f =>

    val uuid = UUID.randomUUID().toString
    val subscribedBusRoute = BusRoute("25", "outbound")
    val (websocketClient, packagesReceivedBuffer) = setUpTestWebSocketClient(uuid, f.wsPort)
    val params = createFilteringParams(busRoutes = List(subscribedBusRoute))

    val openedSocket = websocketClient.open()
    openedSocket ! createJsonStringFromFilteringParams(params)
    parsePacketsReceived(packagesReceivedBuffer) should have size 0

    val subscribedSourceLine = generateSourceLine(route = "25", direction = 1, stopId = "490007497E")
    val nonSubscribedSourceLine = generateSourceLine(route = "3", direction = 2, stopId = "490007190W")
    f.sourceLineHandler.handle(subscribedSourceLine).futureValue
    f.sourceLineHandler.handle(nonSubscribedSourceLine).futureValue

    f.cacheReader ! CacheReadCommand(60000)

    eventually {
      val received = parsePacketsReceived(packagesReceivedBuffer)
      received should have size 1
      received.head.vehicleId shouldBe subscribedSourceLine.vehicleId
      received.head.busRoute shouldBe subscribedBusRoute
    }

    websocketClient.shutdownSync()
  }

  test("When two clients subscribe with same params, they receive the same data") { f =>

    val uuid1 = UUID.randomUUID().toString
    val uuid2 = UUID.randomUUID().toString
    val subscribedBusRoute = BusRoute("25", "outbound")
    val (websocketClient1, packagesReceivedBuffer1) = setUpTestWebSocketClient(uuid1, f.wsPort)
    val (websocketClient2, packagesReceivedBuffer2) = setUpTestWebSocketClient(uuid2, f.wsPort)

    val params = createFilteringParams(busRoutes = List(subscribedBusRoute))
    val openedSocket1 = websocketClient1.open()
    openedSocket1 ! createJsonStringFromFilteringParams(params)
    val openedSocket2 = websocketClient2.open()
    openedSocket2 ! createJsonStringFromFilteringParams(params)

    parsePacketsReceived(packagesReceivedBuffer1) should have size 0
    parsePacketsReceived(packagesReceivedBuffer2) should have size 0

    val sourceLine = generateSourceLine()
    f.sourceLineHandler.handle(sourceLine).futureValue

    f.cacheReader ! CacheReadCommand(60000)

    eventually {
      parsePacketsReceived(packagesReceivedBuffer1) should have size 1
      parsePacketsReceived(packagesReceivedBuffer2) should have size 1
    }

    websocketClient1.shutdownSync()
    websocketClient2.shutdownSync()
  }

  test("When client with same uuid subscribes twice, the second connection recieves an Internal Server Error") { f =>

    val uuid = UUID.randomUUID().toString
    val (websocketClient1, _) = setUpTestWebSocketClient(uuid, f.wsPort)
    websocketClient1.open()

    val (websocketClient2, _) =setUpTestWebSocketClient(uuid, f.wsPort)

    assertThrows[WebSocketHandshakeException] {
      websocketClient2.open()
    }

    websocketClient1.shutdownSync()
    websocketClient2.shutdownSync()
  }
}

