package lbt.web

import java.io.File

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._


class MapService(definitions: Definitions) extends StrictLogging {

  private val supportedAssetTypes = List("css", "js")

  def service = HttpService[IO] {
    case request @ GET -> Root / "assets" / assetType / file if supportedAssetTypes.contains(assetType) =>
      StaticFile.fromFile(new File(s"./src/main/twirl/assets/$assetType/$file"), Some(request))
        .getOrElseF(NotFound())


    case GET -> Root =>
      val busRoutes = definitions.routeDefinitions.map{case (busRoute, _) => busRoute.id}.toList.distinct.sorted
      Ok(html.map(busRoutes))
  }
}
