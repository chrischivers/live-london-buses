package lbt.streaming

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import com.xebialabs.restito.builder.stub.StubHttp.whenHttp
import com.xebialabs.restito.semantics.Action._
import com.xebialabs.restito.semantics.Condition._
import com.xebialabs.restito.server.StubServer
import lbt.ConfigLoader
import org.glassfish.grizzly.http.util.HttpStatus
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{OptionValues, fixture}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

class StreamingClientTest extends fixture.FunSuite with ScalaFutures with OptionValues with Eventually with StrictLogging {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(10 seconds),
    interval = scaled(100 milliseconds)
  )

  case class FixtureParam(streamingClient: StreamingClient, dodgyStreamingClient: StreamingClient, slowStreamingClient: StreamingClient, restitoServer: StubServer, linesReceivedBuffer: ListBuffer[String])

  def withFixture(test: OneArgTest) = {

    val restitoPort = 8000 + Random.nextInt(1000)
    val restitoServer = new StubServer(restitoPort)
    val actorSystem = ActorSystem()
    val modifiedConfig = config.dataSourceConfig.copy(sourceUrl = s"http://localhost:$restitoPort/stream", waitTimeBeforeRestart = 3)
    var linesReceivedBuffer = new ListBuffer[String]()

    def addToBuffer(line: String) = {
      println(line)
      linesReceivedBuffer += line
    }

    def slowAddToBuffer(line: String) = {
      println(line)
      Thread.sleep(500)
      linesReceivedBuffer += line
    }

    var hasFailed = false
    def addToBufferWithErrors(line: String) = {
      if (!hasFailed) {
        logger.info("Exception thrown")
        hasFailed = true
        throw new RuntimeException("Artificial error thrown")
      }
      else {
        println(line)
        linesReceivedBuffer += line
      }
    }

    val streamingClient = new StreamingClient(modifiedConfig, addToBuffer)(actorSystem)
    val slowStreamingClient = new StreamingClient(modifiedConfig, slowAddToBuffer)(actorSystem)
    val dodgyStreamingClient = new StreamingClient(modifiedConfig, addToBufferWithErrors)(actorSystem)

    val testFixture = FixtureParam(streamingClient, dodgyStreamingClient, slowStreamingClient, restitoServer, linesReceivedBuffer)

    try {
      restitoServer.start()
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      restitoServer.stop()
    }
  }

  test("Streaming client should drop first record (header) of stream response") { f =>

    val fullStreamResponse = generateStreamResponse
    setStreamResponse(f.restitoServer, fullStreamResponse)
    val streamResponseFirstLineDropped = fullStreamResponse.split("\n").drop(1).toList
    f.streamingClient.start()
    Thread.sleep(3000) // wait for lines to be processed

    f.linesReceivedBuffer should have size streamResponseFirstLineDropped.size
    f.linesReceivedBuffer.toList shouldBe streamResponseFirstLineDropped
    f.restitoServer.getCalls should have size 1
  }

  test("Streaming client should recover from errors in the 'action' without losing data") { f =>

    val fullStreamResponse = generateStreamResponse
    setStreamResponse(f.restitoServer, fullStreamResponse)
    val streamResponseFirstLineDropped = fullStreamResponse.split("\n").drop(1).toList
    f.dodgyStreamingClient.start()

    eventually {
      f.linesReceivedBuffer should have size streamResponseFirstLineDropped.size
      f.linesReceivedBuffer.toList shouldBe streamResponseFirstLineDropped
      f.restitoServer.getCalls should have size 2
    }
  }


  test("Streaming client should try again if 500 received in stream") { f =>

    val fullStreamResponse = generateStreamResponse
    setStreamResponseError(f.restitoServer)
    val streamResponseFirstLineDropped = fullStreamResponse.split("\n").drop(1).toList
    f.streamingClient.start()

    eventually {
      f.restitoServer.getCalls should have size 1
    }
    setStreamResponse(f.restitoServer, fullStreamResponse)

    eventually {
      f.linesReceivedBuffer should have size streamResponseFirstLineDropped.size
      f.linesReceivedBuffer.toList shouldBe streamResponseFirstLineDropped
      f.restitoServer.getCalls should have size 2
    }
  }

  test("Streaming client should try again if server closes connection") { f =>

    val fullStreamResponse = generateStreamResponse
    setStreamResponse(f.restitoServer, fullStreamResponse)
    val streamResponseFirstLineDropped = fullStreamResponse.split("\n").drop(1).toList
    f.slowStreamingClient.start()
    Thread.sleep(800)
    eventually {
      f.linesReceivedBuffer should have size 1
      f.restitoServer.getCalls should have size 1
    }
    f.restitoServer.stop()

    Thread.sleep(1000)

    f.restitoServer.start()

    eventually {
      f.linesReceivedBuffer should have size streamResponseFirstLineDropped.size
      f.linesReceivedBuffer.toList shouldBe streamResponseFirstLineDropped
      f.restitoServer.getCalls should have size 2
    }
  }

  private def setStreamResponse(server: StubServer, response: String) = {
    whenHttp(server).`match`(
      get("/stream"))
      .`then`(ok(), stringContent(response))
  }

  private def setStreamResponseError(server: StubServer) = {
    whenHttp(server).`match`(
      get("/stream"))
      .`then`(status(HttpStatus.INTERNAL_SERVER_ERROR_500))
  }

  private def generateStreamResponse: String = {
    val time = System.currentTimeMillis()
    s"""|[4,"1.0",1505065818103]
        |[1,"490007497E","25",1,"Ilford","BJ11DUV",${time + 300000}]
        |[1,"490004871W","94",2,"Acton Green","LJ16EWK",${time + 300000}]
        |[1,"490011319HB","286",2,"Cutty Sark","YY14WEP",${time + 300000}]
        |[1,"490012451N","43",2,"Friern Barnet","LK54FWP",${time + 300000}]
        |[1,"490005655E","126",1,"Eltham, High St","YX13AFJ",${time + 300000}]
        |[1,"490013919N","197",2,"Peckham","LJ59LWS",${time + 300000}]
        |[1,"490006083S","493",2,"St Georges Hosp","SN12AVL",${time + 300000}]
      """.stripMargin
  }
}