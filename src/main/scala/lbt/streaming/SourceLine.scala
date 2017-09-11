package lbt.streaming

import com.typesafe.scalalogging.StrictLogging

case class SourceLine(route: String, direction: Int, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)

object SourceLine extends StrictLogging {

  def translateToSourceLine(line: String): Option[SourceLine] = {
    val split = splitLine(line)
    if (arrayCorrectLength(split)) Some(SourceLine(split(1).toUpperCase, split(2).toInt, split(0), split(3), split(4), split(5).toLong))
    else {
      logger.error(s"Source array has incorrect number of elements (${split.length}. 6 expected. Or invalid web page retrieved \n " + split.mkString(","))
      None
    }
  }

  private def splitLine(line: String) = line
    .substring(1,line.length-1) // remove leading and trailing square brackets,
    .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    .map(_.replaceAll("\"","")) //takes out double quotations after split
    .map(_.trim) // remove trailing or leading white space
    .tail // discards the first element (always '1')

  private def arrayCorrectLength(array: Array[String]): Boolean = array.length == 6
}
