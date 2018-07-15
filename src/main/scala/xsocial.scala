import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.CatalystTypeConverters.TimestampConverter

import math._

case class PointOfInterest(
  name: String,
  latitude: Double,
  longitude: Double,
  radius: Double
)

case class DeviceLocation(
  advertiserId: String,
  locationAt: Timestamp,
  latitude: Double,
  longitude: Double
)

//A Cell on a grid identified by coordinates of northeast corner
case class Cell(lat: Int, long: Int)

object XSocial {

  val radiusOfEarth = 6371000.0
  val circumferenceOfEarth = 2*Pi*radiusOfEarth*1000

  def meters(degrees: Double) = (degrees/360)*circumferenceOfEarth
  def degrees(meters: Double) = (360*meters)/circumferenceOfEarth

  def coordinate(degree: Double, cellSize: Double): Int =
    floor(degree/cellSize).toInt

  def normalize(degree: Double, width: Int) = {
    assert(-2*width <= degree && degree <= 2*width)
    val diff = width - abs(degree)
    val withinBounds =  //adjust to within [-width,+width]
      if (diff >= 0) degree
      else signum(degree)*(width+diff)
    width + withinBounds  //adjust to within [0,width]
  }

  def cell(latitude: Double, longitude: Double, cellSize: Double) =  
    Cell(
      coordinate(normalize(latitude, 90), cellSize),
      coordinate(normalize(longitude, 180), cellSize)
    )


  def neighboringCells(point: PointOfInterest, cellSize: Double, radius: Double): Set[Cell] = {
    val delta = degrees(point.radius)+radius
    val cells = for {
      x <- Range.Double(point.latitude-delta, point.latitude+delta, cellSize)
      y <- Range.Double(point.longitude-delta, point.longitude+delta, cellSize) 
    } yield cell(x,y,cellSize)
    cells.toSet
  }


  //Haversine formula.  How is there not a function for this in a library?
  def haversine(p1: (Double, Double), p2: (Double, Double)): Double = {
    val latDistance = p1._1 - p2._1
    val lngDistance = p1._2 - p2._2
    val sinLat = sin(latDistance/2)
    val sinLng = sin(lngDistance/2)
    val a = sinLat * sinLat + cos(p1._1)*cos(p2._1)*sinLng*sinLng
    2*atan2(sqrt(a), sqrt(1-a))*radiusOfEarth
  }

  def distance(l: DeviceLocation, p: PointOfInterest): Double = 
    haversine((toRadians(l.latitude), toRadians(l.longitude)),
      (toRadians(p.latitude), toRadians(p.longitude))
    )

  def interestingRecords(
    interesting: RDD[PointOfInterest],
    locations: RDD[DeviceLocation],
    gridSize: Int = 10000,
    radiusMeters: Int = 50
  ): RDD[DeviceLocation] = {

    val radius = degrees(radiusMeters)
    val cellSize = 360.0/gridSize

    val locationGrid =
      locations.map(p => (cell(p.latitude, p.longitude, cellSize), p))

    val interestingGrid: RDD[(Cell,PointOfInterest)] = interesting.flatMap(p =>
      for (cell <- neighboringCells(p, cellSize, radius))  yield (cell, p)
    )

    val joinedGrid: RDD[(Cell, (DeviceLocation,PointOfInterest))] =
      locationGrid.join(interestingGrid)

    val filtered: RDD[(Cell, (DeviceLocation, PointOfInterest))] =
      joinedGrid.filter { case (_, (location, point)) =>
        distance(location, point) <= radiusMeters
      }

    filtered.values.map(_._1)
  }
}


object XSocialClient {
  def main(args: Array[String]) {

    /*
    if (args.size != 2) {
      println("Required arguments are filenames points of interest and device locations")
    }*/
    val conf: SparkConf =  (new SparkConf()).setAppName("xsocial").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val l1 = List[PointOfInterest](
      PointOfInterest("fred", 100.0, 100.0, 50),
      PointOfInterest("wilma",99.0, 101.0,25)
    )
    val rdd1 = sc.parallelize(l1)
//    rdd1.toDF.write.csv("")
    val l2 = List(
      DeviceLocation("a",new Timestamp(System.currentTimeMillis()),100.0,100.1),
      DeviceLocation("b",new Timestamp(System.currentTimeMillis()),99.0,89.0)
    )
    val rdd2 = sc.parallelize(l2)

    sc.stop()
  }
}
