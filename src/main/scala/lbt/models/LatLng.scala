package lbt.models

case class LatLng(lat: Double, lng: Double) {

  def distanceTo(latLng: LatLng, decimalPrecision: Int = 4) = {
    LatLng.calculateDistanceBetween(this, latLng, decimalPrecision)
  }

  def angleTo(latLng: LatLng) = {
    LatLng.getAngleBetween(this, latLng)
  }
}

case class LatLngBounds(southwest: LatLng, northeast: LatLng) {
  def isWithinBounds(point: LatLng) = {
    LatLng.pointIsWithinBounds(point, this)
  }

}

object LatLng {

  private val R = 6378137 // Earth’s mean radius in meters
  private def rad(x: Double) = x * Math.PI / 180

  private def truncateAt(n: Double, p: Int): Double = {
    val s = math pow(10, p)
    (math floor n * s) / s
  }

  // This function was adapted from code at : http://stackoverflow.com/questions/1502590/calculate-distance-between-two-points-in-google-maps-v3
  def calculateDistanceBetween(fromLatLng: LatLng, toLatLng: LatLng, decimalPrecision: Int): Double = {
    val dLat = rad(toLatLng.lat - fromLatLng.lat)
    val dLong = rad(toLatLng.lng - fromLatLng.lng)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(rad(fromLatLng.lat)) * Math.cos(rad(toLatLng.lat)) *
        Math.sin(dLong / 2) * Math.sin(dLong / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val d = R * c
    truncateAt(d, decimalPrecision)
  }

  def getAngleBetween(fromLatLng: LatLng, toLatLng: LatLng): Int = {
    val lat1x = fromLatLng.lat * Math.PI / 180
    val lat2x = toLatLng.lat * Math.PI / 180
    val dLon = (toLatLng.lng - fromLatLng.lng) * Math.PI / 180

    val y = Math.sin(dLon) * Math.cos(lat2x)
    val x = Math.cos(lat1x) * Math.sin(lat2x) -
      Math.sin(lat1x) * Math.cos(lat2x) * Math.cos(dLon)

    val brng = Math.atan2(y, x)
    val result = ((brng * 180 / Math.PI) + 180) % 360
    result.toInt
  }

  def pointIsWithinBounds(point: LatLng, bounds: LatLngBounds): Boolean = {
    bounds.southwest.lat <= point.lat &&
    bounds.northeast.lat >= point.lat &&
    bounds.southwest.lng <= point.lng &&
    bounds.northeast.lng >= point.lng
  }
}
