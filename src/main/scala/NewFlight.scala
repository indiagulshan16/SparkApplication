import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

case class Flight(legId: String, searchDate: String, flightDate: String, startingAirport: String, destinationAirport: String,
                  fareBasisCode: String, travelDuration: Long, elapsedDays: Int, isBasicEconomy: Boolean,
                  isRefundable: Boolean, isNonStop: Boolean, baseFare: Double, totalFare: Double, seatsRemaining: Int,
                  totalTravelDistance: Int, durationInSecondsIntArray: Array[Int], distanceIntArray: Array[Int],
                  segmentsCabinCodeArray: Array[String], segmentsEquipmentDescriptionArray: Array[String],
                  segmentsAirlineCodeArray: Array[String], segmentsAirlineNameArray: Array[String],
                  segmentsDepartureAirportCodeArray: Array[String], segmentsArrivalAirportCodeArray: Array[String],
                  segmentsArrivalTimeRawArray: Array[String], segmentsArrivalTimeEpochSecondsArray: Array[Long],
                  segmentsDepartureTimeRawArray: Array[String], segmentsDepartureTimeEpochSecondsArray: Array[Long])

object NewFlight {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("FlightDatasetAnalysis")
      .master("local[*]")
      .config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
      .getOrCreate()


    // Import spark implicits for encoders
    import spark.implicits._

    val flightDF = spark.read
      .format("csv")
      .option("inferSchema", "true")
      // .schema(flightSchema)
      .option("header", "true")
      .option("mode", "permissive")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\itineraries.csv")

    val dfWithSplitValues = flightDF.withColumn("durationInSecondsArray", split(col("segmentsDurationInSeconds"), "\\|\\|"))
      .withColumn("distanceArray", split(col("segmentsDistance"), "\\|\\|"))
      .withColumn("durationInSecondsIntArray", expr("transform(durationInSecondsArray, x -> cast(x as int))"))
      .withColumn("distanceIntArray", expr("transform(distanceArray, x -> cast(x as int))"))
      .withColumn("segmentsCabinCodeArray", split(col("segmentsCabinCode"), "\\|\\|"))
      .withColumn("segmentsEquipmentDescriptionArray", split(col("segmentsEquipmentDescription"), "\\|\\|"))
      .withColumn("segmentsAirlineCodeArray", split(col("segmentsAirlineCode"), "\\|\\|"))
      .withColumn("segmentsAirlineNameArray", split(col("segmentsAirlineName"), "\\|\\|"))
      .withColumn("segmentsDepartureAirportCodeArray", split(col("segmentsDepartureAirportCode"), "\\|\\|"))
      .withColumn("segmentsArrivalAirportCodeArray", split(col("segmentsArrivalAirportCode"), "\\|\\|"))
      .withColumn("segmentsArrivalTimeRawArray", split(col("segmentsArrivalTimeRaw"), "\\|\\|"))
      .withColumn("segmentsArrivalTimeEpochSecondsArray", split(col("segmentsArrivalTimeEpochSeconds"), "\\|\\|").cast(ArrayType(LongType)))
      .withColumn("segmentsDepartureTimeRawArray", split(col("segmentsDepartureTimeRaw"), "\\|\\|"))
      .withColumn("segmentsDepartureTimeEpochSecondsArray", split(col("segmentsDepartureTimeEpochSeconds"), "\\|\\|").cast(ArrayType(LongType)))


    val cleaneddf = dfWithSplitValues.drop("durationInSecondsArray", "distanceArray", "segmentsDurationInSeconds", "segmentsDistance",
      "segmentsCabinCode", "segmentsEquipmentDescription", "segmentsAirlineCode", "segmentsAirlineName",
      "segmentsDepartureAirportCode", "segmentsArrivalAirportCode", "segmentsArrivalTimeRaw",
      "segmentsArrivalTimeEpochSeconds", "segmentsDepartureTimeRaw", "segmentsDepartureTimeEpochSeconds")


    val flightsDS = cleaneddf.as[Flight]

    // Show first 10 records of flight data
    flightsDS.show(10)

    // Show schema of flight data
  //  flightsDS.printSchema()

    // Perform basic statistics on numerical columns
    //flightsDS.describe("baseFare", "totalFare", "elapsedDays", "seatsRemaining", "totalTravelDistance").show()

    // Calculate average fare by cabin code
    flightsDS.groupBy("segmentsCabinCodeArray")
      .agg(avg("baseFare").alias("avg_baseFare"), avg("totalFare").alias("avg_totalFare"))
      .show()
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.IntegerType

    // Filter out rows with null values in segmentsAirlineNameArray or travelDuration
    val filteredDF = flightsDS.filter(col("segmentsAirlineNameArray").isNotNull && col("travelDuration").isNotNull)

    // Explode the segmentsAirlineNameArray to create a new row for each airline name
    val explodedDF = filteredDF.select("segmentsAirlineNameArray", "travelDuration")
      .withColumn("airlineName", explode(col("segmentsAirlineNameArray")))
      .filter(col("airlineName").isNotNull) // Filter out rows with null airlineName

    // Calculate average travel duration by airline name
    val resultDF = explodedDF.groupBy("airlineName")
      .agg(avg("travelDuration").cast(IntegerType).alias("avg_travelDuration"))


    // Show the result
    resultDF.show()


    // Count number of non-stop flights
    flightsDS.filter("isNonStop = true").count()

    // Count number of refundable flights
    flightsDS.filter("isRefundable = true").count()

    // Perform data visualization

    // Convert flightDate column to date type
    val parsedData = flightsDS.withColumn("flightDate", to_date(col("flightDate"), "yyyy-MM-dd"))

    // Group by flightDate and calculate average totalFare
    val avgFareByDate = parsedData.groupBy("flightDate")
      .agg(avg("totalFare").alias("avg_totalFare"))
      .orderBy("flightDate")

    // Calculate rolling average totalFare over 7 days
    val rollingAvgFareByDate = avgFareByDate.withColumn("rolling_avg_totalFare",
      avg("avg_totalFare").over(Window.orderBy("flightDate").rowsBetween(-3, 3)))

  }
}
