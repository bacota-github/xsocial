import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.catalyst.CatalystTypeConverters.TimestampConverter


/* Pure RDD implementation.  Divide the map into a coordinate grid and
 * classify each DeviceLocation by the grid location.  For each
 * PointOfInterest, determine all the locations on the grid that could
 * contain devices near to the PointOfInterest.  Use a join to
 * identify all possible candidate pairs of DeviceLocation and
 * PointOfInterest, and then filter using a more accurate distance
 * computation. */

//A Cell on a grid identified by coordinates of northeast corner
case class Cell(lat: Int, long: Int)

object XSocialRDD {
  import XSocial._

  def coordinate(degree: Double, cellSize: Double): Int =
    math.floor(degree/cellSize).toInt

  def normalize(degree: Double, width: Int) = {
    assert(-2*width <= degree && degree <= 2*width)
    val diff = width - math.abs(degree)
    val withinBounds =  //adjust to within [-width,+width]
      if (diff >= 0) degree
      else math.signum(degree)*(width+diff)
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

  def interestingRecords(
    interesting: RDD[PointOfInterest],
    locations: RDD[DeviceLocation],
    gridSize: Int = 10000,
    radiusMeters: Int = 50
  ): RDD[DeviceLocation] = {

    val radius = degrees(radiusMeters)
    val cellSize = 360.0/gridSize

    val locationGrid: RDD[(Cell,DeviceLocation)] =
      locations.map(p => (cell(p.latitude, p.longitude, cellSize), p))

    val interestingGrid: RDD[(Cell,PointOfInterest)] = interesting.flatMap(p =>
      for (cell <- neighboringCells(p, cellSize, radius))  yield (cell, p)
    )

    val joinedGrid: RDD[(Cell, (DeviceLocation, PointOfInterest))] =
      locationGrid.join(interestingGrid)

    val filtered: RDD[(Cell, (DeviceLocation, PointOfInterest))] =
      joinedGrid.filter { case (_, (location, point)) =>
        distance(location, point) <= radiusMeters+point.radius
      }

    filtered.values.map(_._1)
  }
}
