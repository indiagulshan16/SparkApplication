import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Window
import java.time.Duration
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
object FlightDetailsAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("FlightDatasetAnalysis")
      .master("local[*]")
      .config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
      .getOrCreate()


    val flightSchema = StructType(Array(
      StructField("legId", StringType),
      StructField("searchDate", StringType),
      StructField("flightDate", StringType),
      StructField("startingAirport", StringType),
      StructField("destinationAirport", StringType),
      StructField("fareBasisCode", StringType),
      StructField("travelDuration", StringType),
      StructField("elapsedDays", IntegerType),
      StructField("isBasicEconomy", BooleanType),
      StructField("isRefundable", BooleanType),
      StructField("isNonStop", BooleanType),
      StructField("baseFare", DoubleType),
      StructField("totalFare", DoubleType),
      StructField("seatsRemaining", IntegerType),
      StructField("totalTravelDistance", IntegerType),
      StructField("segmentsDepartureTimeEpochSeconds", LongType),
      StructField("segmentsDepartureTimeRaw", StringType),
      StructField("segmentsArrivalTimeEpochSeconds", LongType),
      StructField("segmentsArrivalTimeRaw", StringType),
      StructField("segmentsArrivalAirportCode", StringType),
      StructField("segmentsDepartureAirportCode", StringType),
      StructField("segmentsAirlineName", StringType),
      StructField("segmentsAirlineCode", StringType),
      StructField("segmentsEquipmentDescription", StringType),
      StructField("segmentsDurationInSeconds", LongType),
      StructField("segmentsDistance", IntegerType),
      StructField("segmentsCabinCode", StringType)
    ))

    val flightSchemaData = StructType(Array(
      StructField("legId", StringType),
      StructField("searchDate", StringType),
      StructField("flightDate", StringType),
      StructField("startingAirport", StringType),
      StructField("destinationAirport", StringType),
      StructField("travelDuration", StringType),
      StructField("elapsedDays", IntegerType),
      StructField("isBasicEconomy", BooleanType),
      StructField("isRefundable", BooleanType),
      StructField("isNonStop", BooleanType),
      StructField("baseFare", DoubleType),
      StructField("totalFare", DoubleType),
      StructField("seatsRemaining", IntegerType),
      StructField("totalTravelDistance", IntegerType),
      StructField("segmentsAirlineCode", StringType),
      StructField("segmentsEquipmentDescription", StringType),
      StructField("segmentsCabinCode", StringType),
      StructField("segmentsDepartureTime", StringType),
      StructField("segmentsArrivalTime", StringType),
      StructField("AirlineName", StringType),
      StructField("travelDurationSeconds", LongType),
      StructField("DepartureAirportCode", StringType),
      StructField("ArrivalAirportCode", StringType),
      StructField("FareClass", StringType),
      StructField("FareBasis", StringType)
    ))

    val flightDF = spark.read
      .format("csv")
      // .schema(flightSchema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "permissive")
      //.option("delimiter", "\t")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\itineraries.csv")


    //    val cleanedDf = flightDF.na.drop()
    //
    //
    //    def travelDurationFormatting(travelD: String) = {
    //      val duration = Duration.parse(travelD)
    //      duration.getSeconds
    //    }
    //    import org.apache.spark.sql.functions.udf
    //    //Register the function as UDF
    //    val registerUDF = udf(travelDurationFormatting _)
    //
    //    //Apply the udf in column travelDuration
    //    val formattedDf = cleanedDf.withColumn("travelDurationSeconds", registerUDF(col("travelDuration")))
    //    formattedDf.show(5)
    //
    //    import org.apache.spark.sql.functions._
    //
    //    val percentileDf = formattedDf.groupBy("legId")
    //      .agg(mean("baseFare").alias("meanBaseFare"),
    ////        percentile_approx("baseFare", lit(0.5)).alias("medianBaseFare"),
    //        sum("baseFare").alias("totalBaseFare"),
    //        mean("totalFare").alias("meanTotalFare"),
    ////        percentile_approx("totalFare", lit(0.5)).alias("medianTotalFare"),
    //        sum("totalFare").alias("totalTotalFare"),
    //        mean("travelDurationSeconds").alias("meanTravelDuration"),
    ////        percentile_approx("travelDurationSeconds", lit(0.5)).alias("medianTravelDuration"),
    //        sum("travelDurationSeconds").alias("totalTravelDuration"))
    //    percentileDf.show(5)
    //    // Count the number of distinct values in each column
    //    formattedDf.select(formattedDf.columns.map(c => countDistinct(col(c)).alias(c)): _*).show(5)
    //
    //    // Count the number of missing values in each column
    //    formattedDf.select(formattedDf.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).alias(c)): _*).show(5)
    //
    //    // Count the number of flights by airline
    //    formattedDf.groupBy("segmentsAirlineName").count().orderBy(desc("count")).show(5)
    //
    //    // Average base fare by cabin class
    //    formattedDf.groupBy("segmentsCabinCode").avg("baseFare").orderBy("segmentsCabinCode").show(5)
    //
    //    // Average duration by airline
    //    formattedDf.groupBy("segmentsAirlineName").avg("segmentsDurationInSeconds").orderBy(desc("avg(segmentsDurationInSeconds)")).show(5)
    //
    //    // Distribution of flight duration
    //    formattedDf.select("travelDuration").describe().show()
    //
    //    //Busiest travel dates
    //    formattedDf.groupBy("flightDate").count().orderBy(desc("count")).show()
    //
    ////    //Correlation between fare and distance
    ////    formattedDf.select("totalTravelDistance", "totalFare").corr().show()
    //
    //    //Distribution of fare by airline
    //    formattedDf.groupBy("segmentsAirlineName").agg(avg("totalFare"), stddev("totalFare")).orderBy(desc("avg(totalFare)")).show()
    //
    //    formattedDf.groupBy("isNonStop").count().show()
    //
    //    //Average fare by day of the week
    //    formattedDf.groupBy(date_format(col("flightDate"), "EEEE").alias("day_of_week")).avg("totalFare").orderBy("day_of_week").show()
    //    import org.apache.spark.sql.functions.{regexp_replace, split}
    //
    //    val dfWithDuration = formattedDf.withColumn("durationMinutes",
    //      expr("cast(regexp_extract(travelDuration, '\\d+', 0) as int)"))
    //      .drop("travelDuration")
    //    dfWithDuration.show(5)


    //---------------------------------------------------------------------------------------------


    val flightDFNew = spark.read
      .format("csv")
      // .schema(flightSchema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "permissive")
      //.option("delimiter", "\t")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\NormalizedAirlineData.csv\\AirlineData.csv")
    // Drop any rows with missing values
    val dfClean = flightDFNew.na.drop()

//
//    // Convert data types of some columns
//    val dfCleanTyped = dfClean
//      .withColumn("searchDate", to_date(col("searchDate"), "yyyy-MM-dd"))
//      .withColumn("flightDate", to_date(col("flightDate"), "yyyy-MM-dd"))
//      .withColumn("travelDurationSeconds", col("travelDurationSeconds").cast(IntegerType))
//      .withColumn("baseFare", col("baseFare").cast(DoubleType))
//      .withColumn("totalFare", col("totalFare").cast(DoubleType))
//      .withColumn("seatsRemaining", col("seatsRemaining").cast(IntegerType))
//
//    // Group by starting airport and calculate average base fare and total fare
//    val avgFaresByAirport = dfCleanTyped
//      .groupBy("startingAirport")
//      .agg(avg("baseFare").as("avgBaseFare").cast(DoubleType), avg("totalFare").as("avgTotalFare").cast(DoubleType))
//    // Show some sample data
//    avgFaresByAirport.show(10)
//
//    // Calculate correlation between travel duration and base fare
//    val corr = dfCleanTyped.stat.corr("travelDurationSeconds", "baseFare")
//
//    // Print the correlation coefficient
//    println(s"Correlation between travel duration and base fare: $corr")
//
//    // Plot a histogram of totalFare
//    dfCleanTyped.select("totalFare").rdd.map(r => r.getDouble(0)).histogram(10)
//    // Plot a histogram of the "seatsRemaining" column
//    val seatsHistogram = dfCleanTyped.select("seatsRemaining").rdd.map(r => r(0).asInstanceOf[Int]).histogram(10)
//
//    println("Seats Remaining Histogram:")
//    seatsHistogram._1.zip(seatsHistogram._2).foreach { case (seat, count) =>
//      println(s"$seat - ${seat + seatsHistogram._1(1)}: $count")
//    }




    // Clean the data by dropping rows with missing values and duplicates
    val cleanedData = flightDFNew.na.drop().dropDuplicates()

    // Compute the correlation between baseFare and totalFare
    cleanedData.stat.corr("baseFare", "totalFare")

    // Analyze the data at a weekly granularity
    val weeklyData = flightDFNew.withColumn("week", weekofyear(col("flightDate"))).groupBy("week").agg(avg("baseFare"), count("legId"))
    weeklyData.show(5)
    //Check the distinct values in a column
    flightDFNew.select("startingAirport").distinct().show(5)

    //Check the statistical summary of numerical columns
    flightDFNew.describe("baseFare", "totalFare").show(5)

    //Check the average travel distance by cabin class
    flightDFNew.groupBy("segmentsCabinCode").agg(avg("totalTravelDistance")).show(5)

    //Check the number of flights by airline
    flightDFNew.groupBy("AirlineName").count().orderBy(desc("count")).show(5)

    //Check the number of non-stop flights
    println("Number of non-stop flights" + flightDFNew.filter("isNonStop = true").count())

    //Check the number of flights by starting airport and destination airport
    flightDFNew.groupBy("startingAirport", "destinationAirport").count().orderBy(desc("count")).show(5)

    //Check the number of flights by month
    flightDFNew.withColumn("month", month(col("flightDate")))
      .groupBy("month")
      .count()
      .orderBy("month")
      .show(10)

    //Check the number of flights by day of week
//    flightDFNew.withColumn("dayOfWeek", dayofweek(col("flightDate")))
//      .groupBy("dayOfWeek")
//      .count()
//      .orderBy("dayOfWeek")
//      .show(5)
    flightDFNew.withColumn("dayOfWeek", date_format(date_format(col("flightDate"), "EEEE"), "EEEE"))
      .groupBy("dayOfWeek")
      .count()
      .orderBy("dayOfWeek")
      .show(5)

    //Check the average fare by airline
    flightDFNew.groupBy("AirlineName").agg(avg("totalFare")).orderBy(desc("avg(totalFare)")).show()

    //Check the number of flights by travel duration
    flightDFNew.withColumn("travelDurationSeconds", round(col("travelDurationSeconds") * 60, 2))
      .groupBy("travelDurationSeconds")
      .count()
      .orderBy(desc("count"))
      .show(5)

  //Check the number of flights by refundable status
    flightDFNew.groupBy("isRefundable").count().persist().show(5)

    //Check the percentage of seats remaining
    flightDFNew.select(round(avg(col("seatsRemaining")), 2).alias("avgSeatsRemaining")).persist().show(5)

    //Check the number of flights by equipment description
    flightDFNew.groupBy("segmentsEquipmentDescription").count().orderBy(desc("count")).persist().show(5)

    //Check the percentage of non-stop flights
    val nonStopFlights = flightDFNew.filter(col("isNonStop") === true).count() / flightDFNew.count().toDouble * 100
    println("percentage of non-stop flights is " + nonStopFlights)

    //Check the average travel duration by airline
    flightDFNew.groupBy("AirlineName").agg(avg("travelDurationSeconds")).orderBy(desc("avg(travelDuration)")).persist().show(5)
    spark.stop()

  }
}