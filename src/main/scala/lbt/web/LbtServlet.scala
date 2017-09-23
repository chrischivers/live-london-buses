package lbt.web

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.RedisClient
import lbt.models.BusRoute
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object RouteIdParamMatcher extends QueryParamDecoderMatcher[String]("bounds")

object CategoryQueryParamMatcher extends QueryParamDecoderMatcher[String]("category")

class LbtServlet(redisClient: RedisClient, definitions: Definitions) extends StrictLogging {

  val service = HttpService[IO] {
    case GET -> Root / "lbt" / routeId / direction =>

      val busRoute = BusRoute(routeId, direction)
      logger.debug(s"Http request received for $busRoute")
      definitions.routeDefinitions.get(busRoute).fold(NotFound()) { routeList =>
        val result: Future[List[(Int, String, Double)]] = Future.sequence(routeList.map(stopRec => {
          val timeDiffsOpt = redisClient.getStopToStopTimes(busRoute, stopRec._1, stopRec._1 + 1)
          val averageTimeDiff = timeDiffsOpt.map(list => calculateAverageTimes(list))
          averageTimeDiff.map(av => (stopRec._1, stopRec._2.stopName, av))
        }))
        Ok(result.map(res => html.averagetimes(busRoute, res)))
      }
  }

  private def calculateAverageTimes(timeDiffs: Seq[Int]): Double = {
    if (timeDiffs.isEmpty) 0.0
    else timeDiffs.sum.toDouble / timeDiffs.size.toDouble
  }
}
