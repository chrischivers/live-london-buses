package lbt.streaming

import org.scalatest.{FunSuite, OptionValues}
import org.scalatest.Matchers._

class SourceLineTest extends FunSuite with OptionValues {

  test("Raw source lines can be converted to SourceLine types") {
    val time = System.currentTimeMillis() + 30000
    val line = s"""[1,"490007497E","25",1,"Ilford","BJ11DUV",$time]"""

    val sourceLine = SourceLine.fromRawLine(line).value

    sourceLine.stopID shouldBe "490007497E"
    sourceLine.route shouldBe "25"
    sourceLine.direction shouldBe 1
    sourceLine.destinationText shouldBe "Ilford"
    sourceLine.vehicleID shouldBe "BJ11DUV"
    sourceLine.arrival_TimeStamp shouldBe time
  }

  test("Source Line validation fails if time is in the past") {
    val sourceLine = generateSourceLine(timeStamp = System.currentTimeMillis() - 1000)
    SourceLine.validate(sourceLine) shouldBe false
  }


  def generateSourceLine(
          route: String = "25",
          direction: Int = 1,
          stopId: String = "490007497E",
          destination: String = "Ilford",
          vehicleId: String = "BJ11DUV",
          timeStamp: Long = System.currentTimeMillis()) =
  {
    SourceLine(route, direction, stopId, destination, vehicleId, timeStamp)
  }
}
