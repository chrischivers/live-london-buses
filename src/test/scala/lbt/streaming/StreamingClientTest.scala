package lbt.streaming

import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}

import akka.actor.ActorSystem
import com.xebialabs.restito.builder.stub.StubHttp.whenHttp
import com.xebialabs.restito.semantics.Action._
import com.xebialabs.restito.semantics.Condition._
import com.xebialabs.restito.server.StubServer
import lbt.ConfigLoader
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

class StreamingClientTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(1 second)
  )

  case class FixtureParam(streamingClient: StreamingClient, restitoServer: StubServer, linesReceivedBuffer: ListBuffer[String])

  def withFixture(test: OneArgTest) = {

    val restitoPort =  8000 + Random.nextInt(1000)
    val restitoServer = new StubServer(restitoPort)
    val actorSystem = ActorSystem()
    val modifiedConfig = config.dataSourceConfig.copy(sourceUrl = s"http://localhost:$restitoPort/stream")
    var linesReceivedBuffer = new ListBuffer[String]()

    def addToBuffer(line: String) = {
      println(line)
      linesReceivedBuffer += line
    }

    val dataSourceClient = new BusDataSourceClient(modifiedConfig)
    val streamingClient = new StreamingClient(dataSourceClient, addToBuffer)(actorSystem)

    val testFixture = FixtureParam(streamingClient, restitoServer, linesReceivedBuffer)

    try {
      restitoServer.start()
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      streamingClient.close
      restitoServer.stop()
    }
  }

  test("Streaming client should drop first record (header) of stream response") { f =>

    val fullStreamResponse = generateStreamResponse
    setStreamResponse(f.restitoServer, fullStreamResponse)
    val streamResponseFirstLineDropped = fullStreamResponse.split("\n").drop(1).toList
    f.streamingClient.start().futureValue

    f.linesReceivedBuffer should have size streamResponseFirstLineDropped.size
    f.linesReceivedBuffer.toList shouldBe streamResponseFirstLineDropped
    f.restitoServer.getCalls should have size 1
  }

  private def setStreamResponse(server: StubServer, response: String) = {
    whenHttp(server).`match`(
      get("/stream"))
      .`then`(ok(), stringContent(response))
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