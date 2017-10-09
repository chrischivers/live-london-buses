package lbt.streaming

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.metrics.MetricsLogging
import lbt.models.{BusPolyLine, BusRoute, BusStop}
import lbt.streaming.Vehicle.StopIndex

sealed trait VehicleCommands

case class Handle(stopArrivalRecord: StopArrivalRecord, arrivalTimestamp: Long) extends VehicleCommands
//case class Transmit(stopIndex: StopIndex)
case object GetArrivalTimesMap extends VehicleCommands

object Vehicle {
  type StopIndex = Int
  def generateActorId(vehicleId: String, busRoute: BusRoute) = s"$vehicleId-${busRoute.id}-${busRoute.direction}"
}

class Vehicle(vehicleId: String, busRoute: BusRoute, definitionStopList: List[(StopIndex, BusStop, Option[BusPolyLine])]) extends Actor with StrictLogging {

  val id: String = self.path.name
  logger.debug(s"Starting up new actor for vehicle $vehicleId on route $busRoute. Name: $id")

  override def receive = active(vehicleId, busRoute, definitionStopList, Map.empty)

  def active(vehicleId: String,
             busRoute: BusRoute,
             definitionStopList: List[(Int, BusStop, Option[BusPolyLine])],
             predictedArrivalTimes: Map[StopIndex, Long]): Receive = {

    case Handle(stopArrivalRecord, arrivalTimestamp) => context.become(active(vehicleId, busRoute, definitionStopList, predictedArrivalTimes + (stopArrivalRecord.stopIndex -> arrivalTimestamp)))
    case GetArrivalTimesMap => sender() ! predictedArrivalTimes
  }

  override def postStop(): Unit = {
    MetricsLogging.decrLiveVehicleActors
  }
}
