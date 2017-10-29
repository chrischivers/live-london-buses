package lbt.web

import java.io.File
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import lbt.MapServiceConfig
import lbt.common.Definitions
import lbt.metrics.MetricsLogging
import lbt.models.{BusRoute, BusStop}
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MapsHttpService(mapServiceConfig: MapServiceConfig, definitions: Definitions, mapsClientHandler: MapsClientHandler) extends StrictLogging {

  object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")

  object VehicleIdQueryParameter extends QueryParamDecoderMatcher[String]("vehicleId")

  object RouteIdQueryParameter extends QueryParamDecoderMatcher[String]("routeId")

  object DirectionQueryParameter extends QueryParamDecoderMatcher[String]("direction")

  private val supportedAssetTypes = List("css", "js", "images")

  def service = HttpService[IO] {

    case request@GET -> Root / "assets" / assetType / file if supportedAssetTypes.contains(assetType) =>
      StaticFile.fromFile(new File(s"./src/main/twirl/assets/$assetType/$file"), Some(request))
        .getOrElseF(NotFound())

    case req@POST -> Root / "snapshot" :? UUIDQueryParameter(uuid) =>
      handleSnapshotRequest(uuid, req)

    case req@GET -> Root / "nextstops"
      :? VehicleIdQueryParameter(vehicleId)
      :? RouteIdQueryParameter(routeId)
      :? DirectionQueryParameter(direction) =>

      val busRoute = BusRoute(routeId, direction)
      val busStopsForRoute = definitions.routeDefinitions(busRoute)
      MetricsLogging.incrNextStopsHttpRequestsReceived

      Ok(mapsClientHandler.getNextStopResponse(vehicleId, busRoute, busStopsForRoute))

    case req@GET -> Root / "positions" :? UUIDQueryParameter(uuid) =>
      MetricsLogging.incrPositionsHttpRequestsReceived
      val result = for {
        existsAlready <- mapsClientHandler.isAlreadySubscribed(uuid)
        _ <- if (!existsAlready) mapsClientHandler.subscribe(uuid) else Future.successful()
        transmissionData <- mapsClientHandler.retrieveTransmissionDataForClient(uuid)
      } yield transmissionData

      Ok(result)

    case GET -> Root => handleMapRequest
  }

  private def handleMapRequest = {
    MetricsLogging.incrMapHttpRequestsReceived
    val newUUID = UUID.randomUUID().toString
    val busRoutes = definitions.routeDefinitions.map { case (busRoute, _) => busRoute.id }.toList.distinct
    val (digitRoutes, letterRoutes) = busRoutes.partition(_.forall(_.isDigit))
    val sortedRoutes = digitRoutes.sortBy(_.toInt) ++ letterRoutes.sorted
    Ok(html.map(newUUID, mapServiceConfig, sortedRoutes))
  }

  private def handleSnapshotRequest(uuid: String, req: Request[IO]) = {
    MetricsLogging.incrSnapshotHttpRequestsReceived
    val body = new String(req.body.runLog.unsafeRunSync.toArray, "UTF-8")
    val parseResult = for {
      json <- parse(body)
      filteringParams <- json.as[FilteringParams]
    } yield filteringParams

    parseResult match {
      case Right(fp) => {
        val response: Future[String] = for {
          _ <- mapsClientHandler.updateFilteringParams(uuid, fp)
          inProgressData <- mapsClientHandler.getInProgressDataSatisfying(fp)
          modifiedInProgressData = mapsClientHandler.modifyBusPositionDataToStartNow(inProgressData)
        } yield modifiedInProgressData.asJson.noSpaces
        Ok(response)
      }
      case Left(e) =>
        logger.error(s"Error parsing/decoding json $body. Error: $e")
        InternalServerError()
    }
  }
}

object MapsHttpService {

  case class NextStopResponse(vehicleId: String, busRoute: BusRoute, predictedArrival: Long, stopIndex: Int, busStop: BusStop)

}
