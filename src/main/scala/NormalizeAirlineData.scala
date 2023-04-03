import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.{expr}
import java.time.Duration
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{split}

object NormalizeAirlineData {


    def main(args: Array[String]) {
        val spark = SparkSession.builder()
          .appName("FlightDatasetAnalysis")
          .master("local[*]")
          .config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
          .getOrCreate()


        val flightDF = spark.read
          .format("csv")
          // .schema(flightSchema)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("mode", "permissive")
          //.option("delimiter", "\t")
          .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\itineraries.csv")


        // Extract date and time from timestamp columns
        val dfWithTimestamp = flightDF.withColumn("searchDate", expr("substring_index(searchDate, 'T', 1)"))
          .withColumn("flightDate", expr("substring_index(flightDate, 'T', 1)"))
          .withColumn("segmentsDepartureTime", expr("from_unixtime(segmentsDepartureTimeEpochSeconds)"))
          .withColumn("segmentsArrivalTime", expr("from_unixtime(segmentsArrivalTimeEpochSeconds)"))
        dfWithTimestamp.show(5)

        // Normalize airline name column
        val dfWithAirlineName =  dfWithTimestamp.withColumn("AirlineName", split(col("segmentsAirlineName"), "\\|\\|")(0))
        dfWithAirlineName.show(5)

        // Normalize travel duration column
        def travelDurationFormatting(travelD: String) = {
            val duration = Duration.parse(travelD)
            duration.getSeconds
        }
        //Register the function as UDF
        val registerUDF = udf(travelDurationFormatting _)
        //Apply the udf in column travelDuration
        val formattedDf = dfWithAirlineName.withColumn("travelDurationSeconds", registerUDF(col("travelDuration")))
        formattedDf.show(5)

        // Split segments departure and arrival airport codes into separate columns
//        val dfWithAirportCodes = formattedDf.withColumn("DepartureAirportCode", expr("split(segmentsDepartureAirportCode, '\\|\\|')[0]"))
//          .withColumn("ArrivalAirportCode", expr("split(segmentsArrivalAirportCode, '\\|\\|')[0]"))

        val dfWithAirportCodes = formattedDf
          .withColumn("DepartureAirportCode", split(col("segmentsDepartureAirportCode"), "\\|\\|").getItem(0))
          .withColumn("ArrivalAirportCode", split(col("segmentsArrivalAirportCode"), "\\|\\|").getItem(0))

        dfWithAirportCodes.show(5)

        // Split fare basis code into fare class and fare basis columns
        val dfWithFareBasis = dfWithAirportCodes.withColumn("FareClass", expr("substring(fareBasisCode, 1, 1)"))
          .withColumn("FareBasis", expr("substring(fareBasisCode, 2, length(fareBasisCode)-1)"))
        dfWithFareBasis.show(5)

        // Drop unnecessary columns
        val dfNormalized = dfWithFareBasis.drop("fareBasisCode", "segmentsDepartureAirportCode", "segmentsArrivalAirportCode",
            "segmentsAirlineName", "segmentsDurationInSeconds", "segmentsDistance")
          .drop("segmentsDepartureTimeEpochSeconds", "segmentsArrivalTimeEpochSeconds",
              "segmentsDepartureTimeRaw", "segmentsArrivalTimeRaw")

        dfNormalized.coalesce(1).write.option("header", "true").csv("NormalizedAirlineData.csv")

        dfNormalized.show(5)
    }
}