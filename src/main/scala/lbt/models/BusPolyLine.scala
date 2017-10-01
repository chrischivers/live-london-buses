package lbt.models

case class MovementInstruction(from: LatLng, to: LatLng, angle: Int, proportionalDistance: Double)

case class BusPolyLine(encodedPolyLine: String) {
  def decode: List[LatLng] = BusPolyLine.decodePolyLine(this)
  def toMovementInstructions: List[MovementInstruction] = {
    BusPolyLine.toMovementInstructions(decode)
  }
}

object BusPolyLine {

  private def truncateAt(n: Double, decimalPrecision: Int = 6): Double = {
    val s = math pow(10, decimalPrecision)
    (math floor n * s) / s
  }

  /**
    * The code in this method was adapted from the Java Decode Method of Google's PolyUtil Class from Android Map Utils
    * https://github.com/googlemaps/android-maps-utils/blob/master/library/src/com/google/maps/android/PolyUtil.java
    */
  def decodePolyLine(polyLine: BusPolyLine): List[LatLng] = {
    val encodedPolyLine = polyLine.encodedPolyLine
    val len: Int = encodedPolyLine.length
    var latLngList: List[LatLng] = List()

    var index: Int = 0
    var lat: Int = 0
    var lng: Int = 0

    while (index < len) {
      var result: Int = 1
      var shift: Int = 0
      var b: Int = 0

      do {
        b = encodedPolyLine.charAt(index) - 63 - 1
        index += 1
        result += b << shift
        shift += 5
      } while (b >= 0x1f)


      lat += (if ((result & 1) != 0) ~(result >> 1) else result >> 1)

      result = 1
      shift = 0

      do {
        b = encodedPolyLine.charAt(index) - 63 - 1
        index += 1
        result += b << shift
        shift += 5
      } while (b >= 0x1f)

      lng += (if ((result & 1) != 0) ~(result >> 1) else result >> 1)

      val x = LatLng(lat * 1e-5, lng * 1e-5).truncate()
      latLngList = latLngList :+ x
    }
    latLngList
  }

  def toMovementInstructions(decodedPolyLine: List[LatLng]): List[MovementInstruction] = {

    type temporaryInstructionList = (LatLng, LatLng, Int, Double)

    def helper(remainingToDecode: List[LatLng], accumulatedInstructions: List[temporaryInstructionList]): List[temporaryInstructionList] = {
      remainingToDecode match {
        case Nil => accumulatedInstructions
        case x :: Nil => accumulatedInstructions
        case from :: to :: tail =>
          val acc = accumulatedInstructions :+ (from, to, from.angleTo(to), from.distanceTo(to))
          helper(to :: tail, acc)
      }
    }

    val temporaryResults = helper(decodedPolyLine, List.empty)
    val sumOfAllDistances = temporaryResults.foldLeft(0.0)((acc, x) => acc + x._4)
    temporaryResults.map(res => MovementInstruction(res._1, res._2, res._3, truncateAt(res._4 / sumOfAllDistances)))
  }
}
