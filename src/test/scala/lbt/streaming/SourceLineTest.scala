package lbt.streaming

import org.scalatest.{FunSuite, OptionValues}
import org.scalatest.Matchers._

class SourceLineTest extends FunSuite with OptionValues {

  test("Raw source lines can be converted to SourceLine types") {
    val time = System.currentTimeMillis() + 30000
    val line = s"""[1,"490007497E","25",1,"Ilford","BJ11DUV",$time]"""
    val sourceLine = SourceLine.translateToSourceLine(line).value
    sourceLine.stopID shouldBe "490007497E"
    sourceLine.route shouldBe "25"
    sourceLine.direction shouldBe 1
    sourceLine.destinationText shouldBe "Ilford"
    sourceLine.vehicleID shouldBe "BJ11DUV"
    sourceLine.arrival_TimeStamp shouldBe time
  }


}
