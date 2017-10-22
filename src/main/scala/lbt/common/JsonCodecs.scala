package lbt.common

import io.circe.generic.semiauto._
import io.circe.{Decoder, HCursor}
import lbt.models.{BusStop, LatLng}

object JsonCodecs {

  case class JsonRoute(routeId: String, mode: String, directions: List[String])

  case class JsonDirection(direction: String)

  implicit val decodeDirections: Decoder[JsonDirection] = deriveDecoder

  implicit val decodeBusRoute: Decoder[JsonRoute] = new Decoder[JsonRoute] {
    final def apply(c: HCursor): Decoder.Result[JsonRoute] =
      for {
        id <- c.downField("id").as[String]
        modeName <- c.downField("modeName").as[String]
        direction <- c.downField("routeSections").as[List[JsonDirection]]
      } yield {
        JsonRoute(id, modeName, direction.map(_.direction))
      }
  }

  implicit val decodeBusStop: Decoder[BusStop] = new Decoder[BusStop] {
    final def apply(c: HCursor): Decoder.Result[BusStop] =
      for {
        id <- c.downField("id").as[String]
        name <- c.downField("name").as[String]
        lat <- c.downField("lat").as[Double]
        lng <- c.downField("lon").as[Double]
      } yield {
        BusStop(id, name, LatLng(lat, lng))
      }
  }

  implicit val decodeBusStops = Decoder[List[BusStop]].prepare(_.downField("stopPointSequences").downArray.first.downField("stopPoint"))

}
