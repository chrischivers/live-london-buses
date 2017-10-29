package lbt.web

import com.typesafe.scalalogging.StrictLogging
import io.circe.parser.parse
import lbt.db.caching.{BusPositionDataForTransmission, RedisArrivalTimeLog, RedisSubscriberCache, RedisWsClientCache}
import lbt.models._
import lbt.web.MapsHttpService.NextStopResponse
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import scala.concurrent.{ExecutionContext, Future}

class MapsClientHandler(redisSubscriberCache: RedisSubscriberCache, redisWsClientCache: RedisWsClientCache, redisArrivalTimeLog: RedisArrivalTimeLog)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def subscribe(clientUuid: String) = {
    logger.info(s"Subscribing client $clientUuid")
    redisSubscriberCache.subscribe(clientUuid, None)
  }

  def isAlreadySubscribed(uuid: String): Future[Boolean] = {
    logger.debug(s"Checking if $uuid already exists")
    redisSubscriberCache.subscriberExists(uuid)
  }

  def updateFilteringParams(uuid: String, filteringParams: FilteringParams) = {
    redisSubscriberCache.updateFilteringParameters(uuid, filteringParams)
  }

  def retrieveTransmissionDataForClient(uuid: String): Future[String] = {
    redisWsClientCache.getVehicleActivityJsonForClient(uuid)
  }

  def getInProgressDataSatisfying(filteringParams: FilteringParams): Future[Seq[BusPositionDataForTransmission]] = {
    redisWsClientCache.getVehicleActivityInProgress().map { y =>
      y.flatMap(parseWebsocketCacheResult)
        .filter(_.satisfiesFilteringParams(filteringParams))
        .filter(hasStartedButNotFinished)
        .groupBy(_.vehicleId)
        .flatMap { case (_, records) => records.sortBy(_.nextStopArrivalTime.getOrElse(0L)).reverse.headOption
        }.toList
    }
  }

  def modifyBusPositionDataToStartNow(data: Seq[BusPositionDataForTransmission]) = {
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

  def getNextStopResponse(vehicleId: String, busRoute: BusRoute, busStopsForRoute: Seq[(Int, BusStop, Option[BusPolyLine])]) = {
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

  private def hasStartedButNotFinished(rec: BusPositionDataForTransmission) = {
    val now = System.currentTimeMillis()
    now > rec.startingTime &&
      rec.nextStopArrivalTime.fold(true)(nextStop => nextStop > now)
  }

  private def parseWebsocketCacheResult(str: String): Option[BusPositionDataForTransmission] = {
    parse(str).flatMap(_.as[BusPositionDataForTransmission]).toOption
  }
}
