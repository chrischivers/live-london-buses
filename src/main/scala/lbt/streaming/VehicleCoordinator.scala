package lbt.streaming

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.scalalogging.StrictLogging
import lbt.common.{Commons, Definitions}
import lbt.models.{BusPolyLine, BusRoute, BusStop}
import lbt.streaming.Vehicle.StopIndex
import lbt.streaming.VehicleCoordinator.{LastUpdated, VehicleActorID}

sealed trait VehicleCoordinatorCommands
case object GetLiveVehicleList

object VehicleCoordinator {
  type VehicleActorID = String
  type LastUpdated = Long
}

class VehicleCoordinator(definitions: Definitions)(implicit actorSystem: ActorSystem) extends Actor with StrictLogging {

  logger.info("Starting up vehicle coordinator")

  override def receive = active(Map.empty)

  def active(liveVehicles: Map[VehicleActorID, (ActorRef, LastUpdated)]): Receive = {
    case handle @ Handle(stopArrivalRecord, arrivalTimestamp) =>

      val vehicleId = Vehicle.generateActorId(stopArrivalRecord.vehicleId, stopArrivalRecord.busRoute)
      val stopList = definitions.routeDefinitions(stopArrivalRecord.busRoute)

      liveVehicles.get(vehicleId) match {
        case Some((actorRef, _)) => actorRef ! handle
        case None =>
          val newVehicle = createNewVehicle(vehicleId, stopArrivalRecord.busRoute, stopList)
          newVehicle ! handle
          context.become(active(liveVehicles + (vehicleId -> (newVehicle, System.currentTimeMillis()))))
      }

    case GetLiveVehicleList => sender() ! liveVehicles
  }

  def createNewVehicle(vehicleId: String, busRoute: BusRoute, stopList: List[(StopIndex, BusStop, Option[BusPolyLine])]) = {
    actorSystem.actorOf(Props(new Vehicle(vehicleId, busRoute, stopList)),
      name = vehicleId)
  }
}
