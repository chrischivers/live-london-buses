package lbt.streaming

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import lbt.{ConfigLoader, LBTConfig}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{OptionValues, fixture}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

class StreamingClientIntegrationTest extends fixture.FunSuite with ScalaFutures with OptionValues {

  val config: LBTConfig = ConfigLoader.defaultConfig

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(1 second)
  )

  case class FixtureParam(streamingClient: StreamingClient, linesReceivedBuffer: ListBuffer[String])

  def withFixture(test: OneArgTest) = {

    val actorSystem = ActorSystem()
    var linesReceivedBuffer = new ListBuffer[String]()

    def addToBuffer(line: String) = {
      println(line)
      linesReceivedBuffer += line
    }

    val busDataSourceClient = new BusDataSourceClient(config.dataSourceConfig)
    val streamingClient = new StreamingClient(busDataSourceClient, addToBuffer)(actorSystem)

    val testFixture = FixtureParam(streamingClient, linesReceivedBuffer)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      streamingClient.close
    }
  }

  test("Streaming client should process stream response") { f =>

    assertThrows[TimeoutException] {
      Await.result(f.streamingClient.start(), 5 seconds)
    }
    f.linesReceivedBuffer.size should be > 0
  }

}