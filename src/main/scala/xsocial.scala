/* Code shared between two different solutions of the same problem. */

import java.sql.Timestamp
import math._

trait Point {
  def latitude: Double
  def longitude: Double
}

case class PointOfInterest(
  name: String,
  latitude: Double,
  longitude: Double,
  radius: Double
) extends Point

case class DeviceLocation(
  advertiserId: String,
  locationAt: Timestamp,
  latitude: Double,
  longitude: Double
) extends Point


object XSocial {

  val radiusOfEarth = 6371000.0
  val circumferenceOfEarth = 2*Pi*radiusOfEarth

  def meters(degrees: Double) = (degrees/360)*circumferenceOfEarth
  def degrees(meters: Double) = (360*meters)/circumferenceOfEarth

  //Haversine formula.  How is there not a function for this in a library?
  def haversine(p1: (Double, Double), p2: (Double, Double)): Double = {
    val latDistance = p1._1 - p2._1
    val lngDistance = p1._2 - p2._2
    val sinLat = sin(latDistance/2)
    val sinLng = sin(lngDistance/2)
    val a = sinLat * sinLat + cos(p1._1)*cos(p2._1)*sinLng*sinLng
    abs(2*atan2(sqrt(a), sqrt(1-a)))*radiusOfEarth
  }

  def distance(p1: Point, p2: Point): Double = 
    haversine((toRadians(p1.latitude), toRadians(p1.longitude)),
      (toRadians(p2.latitude), toRadians(p2.longitude))
    )
}
