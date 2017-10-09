package lbt.streaming

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import com.typesafe.scalalogging.StrictLogging
import lbt.StreamingConfig
import lbt.common.{Commons, Definitions}
import lbt.metrics.MetricsLogging
import lbt.models.{BusPolyLine, BusRoute, BusStop}
import lbt.streaming.Vehicle.StopIndex
import lbt.streaming.VehicleCoordinator.{LastUpdated, VehicleActorID}

sealed trait VehicleCoordinatorCommands

case object GetLiveVehicleList extends VehicleCoordinatorCommands

case object CleanupExpiredVehicles extends VehicleCoordinatorCommands

object VehicleCoordinator {
  type VehicleActorID = String
  type LastUpdated = Long
}

class VehicleCoordinator(definitions: Definitions, streamingConfig: StreamingConfig)(implicit actorSystem: ActorSystem) extends Actor with StrictLogging {

  logger.info("Starting up vehicle coordinator")

  override def receive = active(Map.empty, 0)

  def active(liveVehicles: Map[VehicleActorID, (ActorRef, LastUpdated)], linesSinceLastClean: Int): Receive = {
    case handle@Handle(stopArrivalRecord, arrivalTimestamp) =>

      val vehicleId = Vehicle.generateActorId(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute)
      val stopList = definitions.routeDefinitions(stopArrivalRecord.busRoute)

      liveVehicles.get(vehicleId) match {
        case Some((actorRef, _)) => {
          actorRef ! handle
          context.become(active(liveVehicles + (vehicleId -> (actorRef, System.currentTimeMillis())), linesSinceLastClean + 1))
        }
        case None =>
          val newVehicle = createNewVehicle(vehicleId, stopArrivalRecord.busRoute, stopList)
          MetricsLogging.incrLiveVehicleActors
          newVehicle ! handle
          context.become(active(liveVehicles + (vehicleId -> (newVehicle, System.currentTimeMillis())), linesSinceLastClean + 1))
      }

      if (linesSinceLastClean + 1 >= streamingConfig.cleanUpEveryNLines) self ! CleanupExpiredVehicles

    case GetLiveVehicleList => sender() ! liveVehicles
    case CleanupExpiredVehicles =>
      val vehiclesToKeep = cleanUpExpiredVehicles(liveVehicles)
      context.become(active(vehiclesToKeep, linesSinceLastClean = 0))
  }

  def createNewVehicle(vehicleId: String, busRoute: BusRoute, stopList: List[(StopIndex, BusStop, Option[BusPolyLine])]) = {
    actorSystem.actorOf(Props(new Vehicle(vehicleId, busRoute, stopList)),
      name = vehicleId)
  }

  def cleanUpExpiredVehicles(liveVehicles: Map[VehicleActorID, (ActorRef, LastUpdated)]): Map[VehicleActorID, (ActorRef, LastUpdated)] = {
    logger.debug("Cleaning up expired vehicles")
    val cleanUpBefore = System.currentTimeMillis() - streamingConfig.idleTimeBeforeVehicleDeleted.toMillis
    logger.debug(s"cleaning up before $cleanUpBefore")
    val (vehiclesToCleanUp, vehiclesToKeep) = liveVehicles.partition { case (_, (_, lastUpdated)) => lastUpdated < cleanUpBefore }
    val actorsToCleanUp = vehiclesToCleanUp.map { case (_, (actorRef, _)) => actorRef }
    logger.debug(s"${vehiclesToCleanUp.size} vehicles to kill")
    actorsToCleanUp.foreach(actor => actor ! PoisonPill)
    vehiclesToKeep
  }
}
