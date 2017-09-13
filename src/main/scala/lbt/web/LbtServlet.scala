package lbt.web

import com.typesafe.scalalogging.StrictLogging
import lbt.common.Definitions
import lbt.db.RedisClient
import lbt.models.BusRoute
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

class LbtServlet(redisClient: RedisClient, definitions: Definitions) extends StrictLogging {

  val service = HttpService {
    case _@GET -> Root / "lbt" / "3" =>

      val busRoute = BusRoute("3", "outbound")
      definitions.routeDefinitions.get(busRoute).fold(NotFound()) { routeList =>
        val result: List[(Int, String, Double)] = routeList.dropRight(1).map(stopRec => {
          val timeDiffsOpt = redisClient.getStopToStopTimes(busRoute, stopRec._1, stopRec._1 + 1)
          val averageTimeDiffOpt = timeDiffsOpt.map(list => list.sum.toDouble / list.size.toDouble)
          (stopRec._1, stopRec._2.stopName, averageTimeDiffOpt.getOrElse(0.0))
        })

        Ok(html.lbt(result))
      }
  }
}
