package lbt.commons

import lbt.models.{BusPolyLine, LatLng, LatLngBounds}
import org.scalatest.{FunSuite, Matchers}

class GeographicalFeaturesTest extends FunSuite with Matchers {

  test("Bus encoded polyline is decoded into a latlng array") {
    val encodedPolyLine = BusPolyLine("}biyHzoW~KqA")
    encodedPolyLine.decode shouldBe Array(LatLng(51.49759,-0.12558), LatLng(51.49551,-0.12517))
  }

  test("Distance between two latlngs can be calculated with correct precision") {
    val latLng1 = LatLng(51.49759,-0.12558)
    val latLng2 = LatLng(51.49551,-0.12517)

    latLng1.distanceTo(latLng2, 4) shouldBe 233.2814
    latLng1.distanceTo(latLng2, 2) shouldBe 233.28
  }

  test("Angle between two latlngs can be calculated") {
    val latLng1 = LatLng(51.49759,-0.12558)
    val latLng2 = LatLng(51.49551,-0.12517)

    latLng1.angleTo(latLng2) shouldBe 353

  }

  test("Returns true when a latlng is within a set of bounds, and false when it is outside") {

    val latLngBounds = LatLngBounds(LatLng(51.505493, -0.147985), LatLng(51.507890, -0.146183))
    val pointInside = LatLng(51.506995, -0.147556)
    val pointOutside = LatLng(51.509139, -0.145861)
    latLngBounds.isWithinBounds(pointInside) shouldBe true
    latLngBounds.isWithinBounds(pointOutside) shouldBe false
  }

  test("Returns list of movement instructions given a polyline consisting of two points") {

    val latLng1 = LatLng(51.49759,-0.12558)
    val latLng2 = LatLng(51.49551,-0.12517)
    val encodedPolyLine = BusPolyLine("}biyHzoW~KqA")

    val movementInstructions = encodedPolyLine.toMovementInstructions
    movementInstructions should have size 1
    movementInstructions.head.from shouldBe latLng1
    movementInstructions.head.to shouldBe latLng2
    movementInstructions.head.angle shouldBe 353
    movementInstructions.head.proportionalDistance shouldBe 1
  }

  test("Returns list of movement instructions given a polyline consisting of three points") {

    val latLng1 = LatLng(51.51055,-0.121131)
    val latLng2 = LatLng(51.50953,-0.123641)
    val latLng3 = LatLng(51.50849,-0.12561)
    val encodedPolyLine = BusPolyLine("}skyH`tVjEtNnEhK")

    val movementInstructions = encodedPolyLine.toMovementInstructions
    movementInstructions should have size 2
    movementInstructions.head.from shouldBe latLng1
    movementInstructions.head.to shouldBe latLng2
    movementInstructions.head.angle shouldBe 56
    movementInstructions.head.proportionalDistance shouldBe 0.537198

    movementInstructions(1).from shouldBe latLng2
    movementInstructions(1).to shouldBe latLng3
    movementInstructions(1).angle shouldBe 49
    movementInstructions(1).proportionalDistance shouldBe 0.462801
  }

  test("Sum of proportional distances for polyline should be 1") {

    val encodedPolyLine = BusPolyLine("uo`zHdxO`A^d@@RGTMPWF]HuAINS`AAPDXZhA?l@ZJTF@GBUDUPSNQ@I@OGkBE?cl@pmBKMO`@Ob@Ul@J^R`@Xb@f@n@")
    val movementInstructions = encodedPolyLine.toMovementInstructions
    Math.round(movementInstructions.map(_.proportionalDistance).sum * 1000)/1000 shouldBe 1.0000
  }

}