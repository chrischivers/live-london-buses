package lbt.web

import java.io.File

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import lbt.common.Definitions
import lbt.db.caching.{BusPositionDataForTransmission, RedisWsClientCache}
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FilteringParamsQueryMatcher extends QueryParamDecoderMatcher[String]("filteringParams")

class MapService(definitions: Definitions, redisWsClientCache: RedisWsClientCache) extends StrictLogging {

  private val supportedAssetTypes = List("css", "js")

  def service = HttpService[IO] {

    case request @ GET -> Root / "assets" / assetType / file if supportedAssetTypes.contains(assetType) =>
      StaticFile.fromFile(new File(s"./src/main/twirl/assets/$assetType/$file"), Some(request))
        .getOrElseF(NotFound())

    case _ @ GET -> Root / "snapshot" :? FilteringParamsQueryMatcher(params) => {
      //TODO make this POST instead of GET
      val parseResult = for {
        json <- parse(params)
        filteringParams <- json.as[FilteringParams]
      } yield filteringParams

      parseResult match {
        case Right(fp) => Ok(getInProgressData(fp).map(_.asJson.noSpaces))
        case Left(e) =>
          logger.error(s"Error parsing/decoding json $params. Error: $e")
          InternalServerError()
      }
    }

    case GET -> Root =>
      val busRoutes = definitions.routeDefinitions.map{case (busRoute, _) => busRoute.id}.toList.distinct
      val (digitRoutes, letterRoutes) = busRoutes.partition(_.forall(_.isDigit))
      val sortedRoutes = digitRoutes.sortBy(_.toInt) ++ letterRoutes.sorted
      Ok(html.map(sortedRoutes))
  }


  private def getInProgressData(filteringParams: FilteringParams): Future[Seq[BusPositionDataForTransmission]] = {
    val now = System.currentTimeMillis()

    redisWsClientCache.getVehicleActivityInProgress().map { y =>
      y.flatMap(parseWebsocketCacheResult)
        .filter(_.satisfiesFilteringParams(filteringParams))
        .filter(rec => now > rec.startingTime &&
          rec.nextStopArrivalTime.fold(true)(nextStop => nextStop > now))
        .groupBy(_.vehicleId).flatMap { case (_, records) =>
        records.sortBy(_.nextStopArrivalTime.getOrElse(0L)).reverse.headOption
      }.toList
    }
  }

  private def parseWebsocketCacheResult(str: String): Option[BusPositionDataForTransmission] = {
    parse(str).flatMap(_.as[BusPositionDataForTransmission]).toOption
  }

}

//TODO this needs tests

