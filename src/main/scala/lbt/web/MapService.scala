package lbt.web

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._


class MapService() extends StrictLogging {

  def service = HttpService[IO] {
    case GET -> Root =>
      Ok(html.map())
  }
}
