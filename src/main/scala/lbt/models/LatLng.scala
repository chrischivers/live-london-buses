package lbt.models

case class LatLng(lat: Double, lng: Double)

case class LatLngBounds(southwest: LatLng, northeast: LatLng) {

  def isWithinBounds(point: LatLng): Boolean = {
    southwest.lat <= point.lat &&
      northeast.lat >= point.lat &&
      southwest.lng <= point.lng &&
      northeast.lng >= point.lng
  }
}