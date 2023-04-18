import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import java.time.Duration

case class Flight(legId: String, searchDate: String, flightDate: String, startingAirport: String, destinationAirport: String,
                  fareBasisCode: String, travelDuration: String, elapsedDays: Int, isBasicEconomy: Boolean,
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
      .option("header", "true")
      .option("mode", "per" +
        "missive")
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

    println("Total Number of rows in Dataset " + flightsDS.count())

    // Show first 10 records of flight data
    flightsDS.show(10)

    // Show schema of flight data
    flightsDS.printSchema()

    // Perform basic statistics on numerical columns
    flightsDS.describe("baseFare", "totalFare", "elapsedDays", "seatsRemaining", "totalTravelDistance").show()

    // Calculate average fare by cabin code
    flightsDS.groupBy("segmentsCabinCodeArray")
      .agg(avg("baseFare").alias("avg_baseFare"), avg("totalFare").alias("avg_totalFare"))
      .show()

    def travelDurationFormatting(travelD: String) = {
      val duration = Duration.parse(travelD)
      duration.getSeconds
    }

    //Register the function as UDF
    val registerUDF = udf(travelDurationFormatting _)
    //Apply the udf in column travelDuration
    val formattedDf = flightsDS.withColumn("travelDurationSeconds", registerUDF(col("travelDuration")))
    formattedDf.show(5)
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.IntegerType

    // Filter out rows with null values in segmentsAirlineNameArray or travelDuration
    val filteredDF = formattedDf.filter(col("segmentsAirlineNameArray").isNotNull && col("travelDurationSeconds").isNotNull)

    // Explode the segmentsAirlineNameArray to create a new row for each airline name
    val explodedDF = filteredDF.select("segmentsAirlineNameArray", "travelDurationSeconds")
      .withColumn("airlineName", explode(col("segmentsAirlineNameArray")))
      .filter(col("airlineName").isNotNull) // Filter out rows with null airlineName

    // Calculate average travel duration by airline name
    val resultDF = explodedDF.groupBy("airlineName")
      .agg(avg("travelDurationSeconds").cast(LongType).alias("avg_travelDuration"))
    // Show the result
    resultDF.show()

    // Calculate the average travel duration by segments airline name
    val avgTravelDurationByAirline = filteredDF.groupBy("segmentsAirlineNameArray")
      .agg(avg("travelDurationSeconds").alias("avg_travelDuration"))
      .orderBy(desc("avg_travelDuration"))
    avgTravelDurationByAirline.show(5)




    // Count number of non-stop flights
    val countF = flightsDS.filter("isNonStop = true").count()
    println("Number of non-stop flights is " + countF)

    // Count number of refundable flights
   val countFilter =  flightsDS.filter("isRefundable = true").count()
    println("Number of refundable flights is " + countFilter)

    // Convert flightDate column to date type
    val parsedData = flightsDS.withColumn("flightDate", to_date(col("flightDate"), "yyyy-MM-dd"))

    // Group by flightDate and calculate average totalFare
    val avgFareByDate = parsedData.groupBy("flightDate")
      .agg(avg("totalFare").alias("avg_totalFare"))
      .orderBy("flightDate")
       avgFareByDate.show(5)

    // Calculate rolling average totalFare over 7 days
    val rollingAvgFareByDate = avgFareByDate.withColumn("rolling_avg_totalFare",
      avg("avg_totalFare").over(Window.orderBy("flightDate").rowsBetween(-3, 3)))


    // Calculate the percentage of flights that are basic economy
    val basicEconomyPercentage = parsedData.filter(col("isBasicEconomy") === true)
      .agg((count("*") * 100 / parsedData.count()).alias("basic_economy_percentage"))
      .collect()(0)(0).asInstanceOf[Double]
 println("Percentage of flights that are basic economy is " + basicEconomyPercentage)

    // Calculate the average base fare by airline code
    val avgBaseFareByAirline = parsedData.groupBy("segmentsAirlineCodeArray")
      .agg(avg("baseFare").alias("avg_baseFare"))
      .orderBy(desc("avg_baseFare"))
    avgBaseFareByAirline.show(5)

    // Calculate the total number of flights by airline name
    val totalFlightsByAirline = parsedData.groupBy("segmentsAirlineNameArray")
      .agg(count("*").alias("total_flights"))
      .orderBy(desc("total_flights"))
    totalFlightsByAirline.show(5)

    // Calculate the percentage of refundable flights
    val refundableFlightsPercentage = parsedData.filter(col("isRefundable") === true)
      .agg((count("*") * 100 / parsedData.count()).alias("refundable_flights_percentage"))
      .collect()(0)(0).asInstanceOf[Double]
println("The percentage of refundable flights is " + refundableFlightsPercentage)


    // Calculate the percentage of non-stop flights
    val nonStopFlightsPercentage = parsedData.filter(col("isNonStop") === true)
      .agg((count("*") * 100 / parsedData.count()).alias("non_stop_flights_percentage"))
      .collect()(0)(0).asInstanceOf[Double]
    println("The percentage of non-stop flights is" + nonStopFlightsPercentage)

    // Calculate the average seats remaining by airline code
    val avgSeatsRemainingByAirline = parsedData.groupBy("segmentsAirlineCodeArray")
      .agg(avg("seatsRemaining").alias("avg_seatsRemaining"))
      .orderBy(desc("avg_seatsRemaining"))
    avgSeatsRemainingByAirline.show(5)


    // Explode the segmentsAirlineNameArray to create a new row for each airline name
    val explodedAirLineNameDF = filteredDF.select("segmentsAirlineNameArray", "totalTravelDistance")
      .withColumn("airlineName", explode(col("segmentsAirlineNameArray")))
      .filter(col("airlineName").isNotNull) // Filter out rows with null airlineName

    // Calculate the total travel distance by airline name
    val totalTravelDistanceByAirline = explodedAirLineNameDF.groupBy("airlineName")
      .agg(sum("totalTravelDistance").alias("total_travelDistance"))
      .orderBy(desc("total_travelDistance"))
    totalTravelDistanceByAirline.show(5)

    // Calculate the average fare by starting airport
    val avgFareByStartingAirport = parsedData.groupBy("startingAirport")
      .agg(avg("totalFare").alias("avg_totalFare"))
      .orderBy(desc("avg_totalFare"))
    avgFareByStartingAirport.show(5)

    // Calculate the average fare by destination airport
    val avgFareByDestinationAirport = parsedData.groupBy("destinationAirport")
      .agg(avg("totalFare").alias("avg_totalFare"))
      .orderBy(desc("avg_totalFare"))
    avgFareByDestinationAirport.show(5)


    // Calculate the average elapsed days by airline code
    val avgElapsedDaysByAirline = parsedData.groupBy("segmentsAirlineCodeArray")
      .agg(avg("elapsedDays").alias("avg_elapsedDays"))
      .orderBy(desc("avg_elapsedDays"))
    avgElapsedDaysByAirline.show(5)

    // Calculate the total number of flights by starting airport
    val totalFlightsByStartingAirport = parsedData.groupBy("startingAirport")
      .agg(count("*").alias("total_flights"))
      .orderBy(desc("total_flights"))
    totalFlightsByStartingAirport.show(5)

    // Calculate the total number of flights by destination airport
    val totalFlightsByDestinationAirport = parsedData.groupBy("destinationAirport")
      .agg(count("*").alias("total_flights"))
      .orderBy(desc("total_flights"))
    totalFlightsByDestinationAirport.show(5)


    // Calculate the most common cabin code
    val mostCommonCabinCode = parsedData.groupBy("segmentsCabinCodeArray")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
      .limit(1)
    mostCommonCabinCode.show(5)

    // Calculate the most common equipment description
    val mostCommonEquipmentDescription = parsedData.groupBy("segmentsEquipmentDescriptionArray")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
      .limit(1)
    mostCommonEquipmentDescription.show(5)

    // Calculate the most common airline name
        val explodedAirLineName = filteredDF.select("segmentsAirlineNameArray")
          .withColumn("airlineName", explode(col("segmentsAirlineNameArray")))
          .filter(col("airlineName").isNotNull) // Filter out rows with null airlineName

    val mostCommonAirlineName = explodedAirLineName.groupBy("airlineName")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
      .limit(1)
    mostCommonAirlineName.show(5)

    // Calculate the maximum and minimum base fare
    val maxMinBaseFare = parsedData.agg(max("baseFare").alias("max_baseFare"), min("baseFare").alias("min_baseFare"))
    maxMinBaseFare.show(5)

    // Calculate the total number of flights by airline code
    val totalFlightsByAirlineCode = parsedData.groupBy("segmentsAirlineCodeArray")
      .agg(count("*").alias("total_flights"))
      .orderBy(desc("total_flights"))
    totalFlightsByAirlineCode.show(5)


    // Extract month and year from searchDate
    val flightsWithMonthYear = flightsDS.withColumn("month", month(to_date(col("searchDate"), "yyyy-MM-dd")))
      .withColumn("year", year(to_date(col("searchDate"), "yyyy-MM-dd")))

    // Group by month and year and calculate average baseFare
    val fareAnalysisDF = flightsWithMonthYear.groupBy("month", "year")
      .agg(avg("baseFare").alias("avg_baseFare"))
      .orderBy("year", "month")

    // Show the fare analysis results
    fareAnalysisDF.show(5)


    // Extract month and year from searchDate column
    val flightsWithMonthYear1 = flightsDS.withColumn("searchMonth", month(col("searchDate")))
      .withColumn("searchYear", year(col("searchDate")))

    // Calculate average baseFare by month and year
    val avgBaseFareByMonthYear1 = flightsWithMonthYear1.groupBy("searchMonth", "searchYear")
      .agg(avg("baseFare").alias("avg_baseFare"))
      .orderBy("searchYear", "searchMonth")

    // Show the results
    avgBaseFareByMonthYear1.show()


    // Extract month from searchDate column
    val flightsWithMonth2 = flightsDS.withColumn("searchMonth", month(col("searchDate")))

    // Calculate average baseFare by month
    val avgBaseFareByMonth = flightsWithMonth2.groupBy("searchMonth")
      .agg(avg("baseFare").alias("avg_baseFare"))
      .orderBy("searchMonth")

    // Show the results
    avgBaseFareByMonth.show()
    // Extract distinct months from the "searchMonth" column
    val distinctMonths = flightsDS.select("searchMonth")
      .distinct()
      .collect()
      .map(row => row.getAs[Int]("searchMonth"))
      .toList

    // Print the distinct months
    println("Distinct months in the dataset:")
    distinctMonths.foreach(println)

    // Check if all 12 months are present
    val allMonthsPresent = (1 to 12).forall(distinctMonths.contains)
    if (allMonthsPresent) {
      println("Data for all 12 months is present in the dataset.")
    } else {
      println("Data for some months is missing in the dataset.")
    }
 //Calculate total number of rows in the dataset
val totalRows = flightsDS.count()

    // Loop through each column in the dataset
    for (column <- flightsDS.columns) {
      // Calculate the number of null values in the column
      val nullCount = flightsDS.filter(col(column).isNull).count()

      // Calculate the percentage of null values in the column
      val nullPercentage = (nullCount.toDouble / totalRows) * 100

      // Print the results
      println(s"Column: $column")
      println(s"Number of Null Values: $nullCount")
      println(s"Percentage of Null Values: $nullPercentage%")
      println("-------------------------------------------------")
    }

    // Extract month and year from the date column
    val flightsDSWithMonthYear = flightsDS.withColumn("month", month(col("searchDate")))
      .withColumn("year", year(col("searchDate")))

    // Group by month and year, and calculate average base fare
    val avgBaseFareByMonthYear = flightsDSWithMonthYear.groupBy("month", "year")
      .agg(avg("baseFare").alias("avg_base_fare"))
      .orderBy("year", "month")
    // Print the results
    println("Average Base Fare by Month and Year:")
    avgBaseFareByMonthYear.show()
  }
}
