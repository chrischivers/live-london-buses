package lbt.web

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.ActorSystem
import cats.effect.IO
import cats.implicits._
import com.github.andyglow.websocket.WebsocketClient
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream._
import fs2.{Scheduler, Stream}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.parse
import lbt.common.Definitions
import lbt.db.caching.{BusPositionDataForTransmission, RedisDurationRecorder, RedisSubscriberCache, RedisWsClientCache}
import lbt.db.sql.{PostgresDB, RouteDefinitionSchema, RouteDefinitionsTable}
import lbt.models.{BusRoute, LatLng, LatLngBounds}
import lbt.scripts.BusRouteDefinitionsUpdater
import lbt.{ConfigLoader, LBTConfig}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.util.StreamApp
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random
import io.circe.generic.auto._
import io.circe.syntax._
import lbt.streaming.{SourceLine, SourceLineHandler}

import scalacache.ScalaCache
import scalacache.guava.GuavaCache

class WebSocketServiceTest extends fixture.FunSuite with ScalaFutures with OptionValues with BeforeAndAfterAll with EitherValues with StrictLogging with Eventually {

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  val config: LBTConfig = ConfigLoader.defaultConfig
  val portIncrementer = new AtomicInteger(8000)

  implicit val busRouteDecoder: Decoder[BusRoute] = deriveDecoder[BusRoute]
  implicit val busPosDataDecoder: Decoder[BusPositionDataForTransmission] = deriveDecoder[BusPositionDataForTransmission]

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(15 seconds),
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

  case class FixtureParam(redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache, sourceLineHandler: SourceLineHandler, wsPort: Int)

  def withFixture(test: OneArgTest) = {

    val cache = ScalaCache(GuavaCache())
    val wsPort: Int = portIncrementer.incrementAndGet()

    implicit val actorSystem: ActorSystem = ActorSystem()
    val redisConfig = config.redisDBConfig.copy(dbIndex = 1)
    val redisDurationRecorder = new RedisDurationRecorder(redisConfig)
    val redisSubscriberCache = new RedisSubscriberCache(redisConfig) // 1 = test, 0 = main
    val redisWsClientCache = new RedisWsClientCache(redisConfig, redisSubscriberCache)
    val webSocketClientHandler = new WebSocketClientHandler(redisSubscriberCache, redisWsClientCache)
    val webSocketService: WebSocketService = new WebSocketService(webSocketClientHandler)

    val sourceLineHandler = new SourceLineHandler(definitions,config.sourceLineHandlerConfig,redisDurationRecorder, webSocketClientHandler)(cache, ec)


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

    val testFixture = FixtureParam(redisWsClientCache, redisSubscriberCache, sourceLineHandler, wsPort)
    try {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      redisSubscriberCache.flushDB.futureValue
      redisWsClientCache.flushDB.futureValue
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

    val posData1 = createBusPositionData(timeStamp = System.currentTimeMillis() - 10000)
    val posData2 = createBusPositionData(timeStamp = System.currentTimeMillis())

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

  test("Client receives data only for routes they are subscribed to") { f =>

    val uuid = UUID.randomUUID().toString
    val subscribedBusRoute = BusRoute("25", "outbound")
    val (websocketClient, packagesReceivedBuffer) = setUpTestWebSocketClient(uuid, f.wsPort)
    val params = createFilteringParams(busRoutes = List(subscribedBusRoute))

    val openedSocket = websocketClient.open()
    openedSocket ! createJsonStringFromFilteringParams(params)
    parsePacketsReceived(packagesReceivedBuffer) should have size 0

    val subscribedSourceLine = generateSourceLine(route = "25", direction = 1, stopId = "490007497E")
    val nonSubscribedSourceLine = generateSourceLine(route = "3", direction = 2, stopId = "490007190W")
    f.sourceLineHandler.handle(subscribedSourceLine).value.futureValue
    f.sourceLineHandler.handle(nonSubscribedSourceLine).value.futureValue

    eventually {
      val received = parsePacketsReceived(packagesReceivedBuffer)
      received should have size 1
      received.head.vehicleId shouldBe subscribedSourceLine.vehicleID
      received.head.busRoute shouldBe subscribedBusRoute
    }

    websocketClient.shutdownSync()
  }


  private def createBusPositionData(vehicleId: String = Random.nextString(10),
                                    busRoute: BusRoute = BusRoute("3", "outbound"),
                                    lat: Double = 51.4217,
                                    lng: Double = -0.077507,
                                    nextStopName: String = "NextStop",
                                    timeStamp: Long = System.currentTimeMillis()) = {
    BusPositionDataForTransmission(vehicleId, busRoute, lat, lng, nextStopName, timeStamp)
  }

  private def createFilteringParams(busRoutes: List[BusRoute] = List(BusRoute("3", "outbound")),
                                    latLngBounds: LatLngBounds = LatLngBounds(LatLng(51,52), LatLng(52,53))) = {
    FilteringParams(busRoutes, latLngBounds)
  }

  private def parsePacketsReceived(msgs: ListBuffer[String]): List[BusPositionDataForTransmission] = {
    msgs.flatMap { msg =>
      parse(msg).right.value.as[List[BusPositionDataForTransmission]].right.value
    }.toList
  }

  private def createJsonStringFromFilteringParams(filteringParams: FilteringParams): String = {
    filteringParams.asJson.noSpaces
  }

  private def generateSourceLine(
                          route: String = "25",
                          direction: Int = 1,
                          stopId: String = "490007497E",
                          destination: String = "Ilford",
                          vehicleId: String = "BJ11DUV",
                          timeStamp: Long = System.currentTimeMillis() + 30000) = {
    SourceLine(route, direction, stopId, destination, vehicleId, timeStamp)
  }

}

