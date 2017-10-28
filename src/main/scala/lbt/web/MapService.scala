package lbt.web

import java.io.File
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import fs2.Scheduler
import html.map
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import lbt.MapServiceConfig
import lbt.common.Definitions
import lbt.db.caching.{BusPositionDataForTransmission, RedisArrivalTimeLog, RedisSubscriberCache, RedisWsClientCache}
import lbt.metrics.MetricsLogging
import lbt.models.{BusRoute, BusStop, LatLng, MovementInstruction}
import lbt.web.MapService.NextStopResponse
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MapService(mapServiceConfig: MapServiceConfig, definitions: Definitions, redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache, redisArrivalTimeLog: RedisArrivalTimeLog) extends StrictLogging {

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
        Ok(handleNextStopsRequest(vehicleId, busRoute))

    case GET -> Root => handleMapRequest
  }

  private def handleNextStopsRequest(vehicleId: String, busRoute: BusRoute): Future[String] = {

    val busStopsForRoute = definitions.routeDefinitions(busRoute)
    MetricsLogging.incrNextStopsHttpRequestsReceived

    redisArrivalTimeLog.getArrivalRecordsFor(vehicleId, busRoute
    ).map { arrivalRecords =>
      arrivalRecords.flatMap { case (arrivalRecord, time) =>
        busStopsForRoute
          .find(_._1 == arrivalRecord.stopIndex).map { busStop =>
          NextStopResponse(
            arrivalRecord.vehicleId,
            arrivalRecord.busRoute,
            time,
            arrivalRecord.stopIndex,
            busStop._2
          )
        }
      }.asJson.noSpaces
    }
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
          _ <- redisSubscriberCache.updateFilteringParameters(uuid, fp)
          inProgressData <- getInProgressDataSatisfying(fp)
          modifiedInProgressData = modifyBusPositionDataToStartNow(inProgressData)
        } yield modifiedInProgressData.asJson.noSpaces
        Ok(response)
      }
      case Left(e) =>
        logger.error(s"Error parsing/decoding json $body. Error: $e")
        InternalServerError()
    }
  }

  private def modifyBusPositionDataToStartNow(data: Seq[BusPositionDataForTransmission]) = {
    data.map(rec =>
      rec.movementInstructionsToNext.fold(rec) { movementInstructions =>
        val timeToTravel = rec.nextStopArrivalTime.getOrElse(90000L) - rec.startingTime
        val lateBy = System.currentTimeMillis() - rec.startingTime
        val proportionRemaining = 1.0 - (lateBy.toDouble / timeToTravel.toDouble)

        def getInstructionsRemaining(remainingList: List[MovementInstruction], accList: List[MovementInstruction], accProportions: Double): (LatLng, List[MovementInstruction]) = {
          if (remainingList.isEmpty) (rec.startingLatLng, accList)
          else {
            val nextInstruction = remainingList.last
            val nextProportion = accProportions + nextInstruction.proportion
            if (nextProportion > proportionRemaining) (nextInstruction.to, accList)
            else {
              getInstructionsRemaining(remainingList.dropRight(1), nextInstruction +: accList, nextProportion)
            }
          }
        }

        val (startingLatLng, instructionsToNext) = getInstructionsRemaining(movementInstructions, List.empty, 0)
        val sumOfAllProportions = instructionsToNext.foldLeft(0.0)((acc, ins) => acc + ins.proportion)
        val adjustedInstructionsToNext = instructionsToNext.map(ins => ins.copy(proportion = ins.proportion / sumOfAllProportions))
        rec.copy(startingTime = System.currentTimeMillis(), startingLatLng = startingLatLng, movementInstructionsToNext = Some(adjustedInstructionsToNext))
      })
  }


  private def getInProgressDataSatisfying(filteringParams: FilteringParams): Future[Seq[BusPositionDataForTransmission]] = {
    redisWsClientCache.getVehicleActivityInProgress().map { y =>
      y.flatMap(parseWebsocketCacheResult)
        .filter(_.satisfiesFilteringParams(filteringParams))
        .filter(hasStartedButNotFinished)
        .groupBy(_.vehicleId)
        .flatMap { case (_, records) => records.sortBy(_.nextStopArrivalTime.getOrElse(0L)).reverse.headOption
        }.toList
    }
  }

  private def hasStartedButNotFinished(rec: BusPositionDataForTransmission) = {
    val now = System.currentTimeMillis()
    now > rec.startingTime &&
      rec.nextStopArrivalTime.fold(true)(nextStop => nextStop > now)
  }

  private def parseWebsocketCacheResult(str: String): Option[BusPositionDataForTransmission] = {
    parse(str).flatMap(_.as[BusPositionDataForTransmission]).toOption
  }
}

object MapService {

  case class NextStopResponse(vehicleId: String, busRoute: BusRoute, predictedArrival: Long, stopIndex: Int, busStop: BusStop)

}
