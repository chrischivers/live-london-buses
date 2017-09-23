package lbt.db

sealed trait Schema {
  val tableName: String
  val primaryKey: List[String]
}
case class RouteDefinitionSchema(
                             tableName: String = "route_definitions",
                             routeId: String = "route_id",
                             direction: String = "direction",
                             sequence: String = "sequence",
                             stopId: String = "stop_id",
                             stopName: String = "stop_name",
                             lat: String = "lat",
                             lng: String = "lng",
                             polyline_to_next: String = "polyline_to_next",
                             lastUpdated: String = "last_updated") extends Schema {
  override val primaryKey: List[String] = List(routeId, direction, sequence)
}