import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }
//import org.apache.spark.sql.catalyst.CatalystTypeConverters.TimestampConverter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


object XSocialClient {

  def main(args: Array[String]) {
    if (args.size != 2) {
      println("Arguments should be file names for points of interest and device locations, respectively.")
      System.exit(1)
    }

    val pointsOfInterestFileName = args(0)
    val deviceLocationsFileName = args(1)

    val spark: SparkSession = SparkSession.builder().appName("xsocial")
        .config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val schema = ScalaReflection.schemaFor[PointOfInterest]
      .dataType.asInstanceOf[StructType]

    val interestingDS: Dataset[PointOfInterest] =
      spark.read.schema(schema).csv(pointsOfInterestFileName).as[PointOfInterest]

    val locationDS =
      spark.read.parquet(deviceLocationsFileName).as[DeviceLocation]

    val ds = XSocialSql.interestingRecords(spark, interestingDS, locationDS)
    val results = ds.collect()
    spark.stop()

    println("Devices found:")
    results.foreach { println(_) }
  }
}
