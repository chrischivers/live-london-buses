package lbt.common

import lbt.models.LatLng
import org.joda.time.DateTime

object Commons {

  def toDirection(directionInt: Int): String = {
    directionInt match {
      case 1 => "outbound"
      case 2 => "inbound"
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionInt"
      )
    }
  }

  def getSecondsOfWeek(journeyStartTime: Long): Int = {
    val dateTime = new DateTime(journeyStartTime)
    ((dateTime.getDayOfWeek - 1) * 86400) + dateTime.getSecondOfDay
  }
}

