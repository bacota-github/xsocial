/* Spark SQL based implementation using a BroadcastNestedJoin */

import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.catalyst.CatalystTypeConverters.TimestampConverter

case class PointOfInterestWithDegrees(
  latitude: Double,
  longitude: Double,
  radius: Double,
  radiusDegrees: Double
) extends Point

object XSocialSql {
  import XSocial._

  def interestingRecords(
    spark: SparkSession,
    interesting: Dataset[PointOfInterest],
    locations: Dataset[DeviceLocation],
    radiusMeters: Int = 50
  ): Dataset[DeviceLocation] = {

    import spark.implicits._

    //Decorate Points of interest with radius of each point converted to degree
    val interestingWithDegrees: Dataset[PointOfInterestWithDegrees] =
      interesting.map( p => PointOfInterestWithDegrees(
        p.latitude, p.longitude, p.radius, (p.radius)
      ))

    //Columns to be used for joining DeviceLocations with (decorated)
    //PointsOfInterest
    val latDiff = locations("latitude") - interestingWithDegrees("latitude")
    val longDiff = locations("longitude") - interestingWithDegrees("longitude")
    val maxDiff = interestingWithDegrees("radiusDegrees")+degrees(radiusMeters)

    //Use a join to find all pairs (DeviceLocation,PointOfInterest)
    //near enough to be candidates
    val neighbors: Dataset[(DeviceLocation,PointOfInterestWithDegrees)] =
      locations.joinWith(interestingWithDegrees,
        (latDiff < maxDiff) && (latDiff > -maxDiff) &&
          (longDiff < maxDiff) && (longDiff > -maxDiff)
      )

    //Filter using a stricter distance check. Then return just the
    //DeviceLocation component of each pair
    neighbors.filter( (p: (DeviceLocation,PointOfInterestWithDegrees)) =>
      distance(p._1,p._2) <= radiusMeters+p._2.radius
    ).map(_._1)
  }
}
