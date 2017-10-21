package lbt.web

import java.io.File

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import fs2.Scheduler
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import lbt.common.Definitions
import lbt.db.caching.{BusPositionDataForTransmission, RedisSubscriberCache, RedisWsClientCache}
import lbt.models.{LatLng, MovementInstruction}
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class MapService(definitions: Definitions, redisWsClientCache: RedisWsClientCache, redisSubscriberCache: RedisSubscriberCache) extends StrictLogging {

  object UUIDQueryParameter extends QueryParamDecoderMatcher[String]("uuid")
  private val supportedAssetTypes = List("css", "js")

  def service = HttpService[IO] {

    case request@GET -> Root / "assets" / assetType / file if supportedAssetTypes.contains(assetType) =>
      StaticFile.fromFile(new File(s"./src/main/twirl/assets/$assetType/$file"), Some(request))
        .getOrElseF(NotFound())

    case req@POST -> Root / "snapshot" :? UUIDQueryParameter(uuid) =>
      handleSnapshotRequest(uuid, req)
    case GET -> Root => handleMapRequest
  }

  private def handleMapRequest = {
    val busRoutes = definitions.routeDefinitions.map { case (busRoute, _) => busRoute.id }.toList.distinct
    val (digitRoutes, letterRoutes) = busRoutes.partition(_.forall(_.isDigit))
    val sortedRoutes = digitRoutes.sortBy(_.toInt) ++ letterRoutes.sorted
    Ok(html.map(sortedRoutes))
  }

  private def handleSnapshotRequest(uuid: String, req: Request[IO]) = {
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
        println("Proportion remaining: " + proportionRemaining)

        def getInstructionsRemaining(remainingList: List[MovementInstruction], accList: List[MovementInstruction], accProportions: Double): (LatLng, List[MovementInstruction]) = {
          println("Remaining List: " + remainingList + ". accList: " + accList + ". accProportions: " + accProportions)
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
        val sumOfAllProportions = instructionsToNext.foldLeft(0.0)((acc,ins) => acc + ins.proportion)
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

//TODO this needs tests

