import org.scalatest._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.catalyst.CatalystTypeConverters.TimestampConverter


object TestData {

  val radius = 50
  val radiusDegrees = XSocial.degrees(radius)

  def generateData: (List[PointOfInterest], List[DeviceLocation], Set[DeviceLocation]) = {
    import scala.collection.mutable.{Set => mutableSet}
    val ts = new java.sql.Timestamp(1000l)
    val points = mutableSet[PointOfInterest]()
    val locations = mutableSet[DeviceLocation]()
    val expected = mutableSet[DeviceLocation]()

    points += PointOfInterest("North Pole", 90, 0, 10)
    locations += DeviceLocation("South Pole", ts, -90, 0)

    points += PointOfInterest("North", 45, 120, 10)
    locations += DeviceLocation("South", ts, -45, -60)

    points += PointOfInterest("Coloc", 1, 1, 1)
    locations += DeviceLocation("Coloc", ts, 1, 1)
    expected += DeviceLocation("Coloc", ts, 1, 1)

    points += PointOfInterest("Nearby", 3, 2, 1) 
    val nearbyLoc = DeviceLocation("Nearby", ts, 3, 2+0.5*radiusDegrees)
    locations += nearbyLoc
    expected += nearbyLoc

    points += PointOfInterest("InRadius", 10, 10, 2*radius)
    val inradiusLoc = DeviceLocation("InRadius", ts, 10, 10+2.9*radiusDegrees)
    locations += inradiusLoc
    expected += inradiusLoc

    points += PointOfInterest("OutOfRadius", -9, -10, 2*radius)
    val outradiusLoc = DeviceLocation("OutOfRadius", ts, -9, -10-3.1*radiusDegrees)
    locations +=  outradiusLoc

    points += PointOfInterest("InRadius2", 15, 15, radius)
    val inradiusLoc2 = DeviceLocation("InRadius", ts,
      15+1.2*radiusDegrees, 15+1.2*radiusDegrees)
    locations += inradiusLoc2
    expected += inradiusLoc2

    points += PointOfInterest("OutOfRadius2", -15, -15, radius)
    val outradiusLoc2 = DeviceLocation("OutOfRadius2", ts,
      -15+1.5*radiusDegrees, -15+1.5*radiusDegrees)
    locations += outradiusLoc2

    (points.toList, locations.toList, expected.toSet)
  }
}

class TestXSocial extends FunSuite {

  test("rdd method produces expected results") {
    val (points, locations, expected) = TestData.generateData
    val conf: SparkConf =  (new SparkConf()).setAppName("xsocial").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val pointsRDD = sc.parallelize(points)
    val locRDD = sc.parallelize(locations)
    val rdd = XSocialRDD.interestingRecords(pointsRDD, locRDD, TestData.radius)
    val results = rdd.collect().toSet
    sc.stop()
    assert(results == expected)
  }


  test("sql method produces expected results") {
    val (points, locations, expected) = TestData.generateData
    val spark: SparkSession =  SparkSession .builder()
      .appName("xsocial").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val pointsDS = points.toDS
    val locationDS = locations.toDS
    val ds = XSocialSql.interestingRecords(spark, pointsDS, locationDS)
    val results = ds.collect().toSet
    spark.stop()
    assert(results == expected)
  }

}
