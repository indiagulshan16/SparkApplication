# Flight Dataset Analysis

This is a Spark application written in Scala for analyzing a flight dataset. 
It performs various operations on the dataset, including data cleaning, basic statistics, and aggregation. 
The main purpose of the code is to demonstrate how to use Spark's DataFrame and Dataset API for data analysis.

# Here is the description of each column in the CSV data:

- **legId**: Unique identifier for each leg of the flight.
- **searchDate**: The date when the flight search was performed.
- **flightDate**: The date of the actual flight.
- **startingAirport**: The airport code for the departure airport.
- **destinationAirport**: The airport code for the arrival airport.
- **fareBasisCode**: The fare basis code for the flight.
- **travelDuration**: The total duration of the flight including layovers, in human-readable format.
- **elapsedDays**: The number of days between the search date and the flight date.
- **isBasicEconomy**: Indicates if the fare is for basic economy class (True/False).
- **isRefundable**: Indicates if the fare is refundable (True/False).
- **isNonStop**: Indicates if the flight is non-stop (True/False).
- **baseFare**: The base fare of the flight.
- **totalFare**: The total fare of the flight including taxes and fees.
- **seatsRemaining**: The number of seats remaining on the flight.
- **totalTravelDistance**: The total travel distance of the flight in miles.
- **segmentsDepartureTimeEpochSeconds**: The departure time of each flight segment in epoch seconds.
- **segmentsDepartureTimeRaw**: The departure time of each flight segment in human-readable format.
- **segmentsArrivalTimeEpochSeconds**: The arrival time of each flight segment  in epoch seconds.
- **segmentsArrivalTimeRaw**: The arrival time of each flight segment in human-readable format.
- **segmentsArrivalAirportCode**: The airport code for the arrival airport of each flight segment.
- **segmentsDepartureAirportCode**: The airport code for the departure airport of each flight segment.
- **segmentsAirlineName**: The name of the airline operating each flight segment.
- **segmentsAirlineCode**: The code of the airline operating each flight segment.
- **segmentsEquipmentDescription**: The description of the aircraft used for each flight segment.
- **segmentsDurationInSeconds**: The duration of each flight segment in seconds.
- **segmentsDistance**: The distance of each flight segment  in miles.
- **segmentsCabinCode**: The cabin code for each flight segment (e.g., coach, business, first class).

### Total Number of rows in Dataset 6364153

### Code Overview
Code Overview
The code performs the following operations on the flight dataset:

- Reads the flight dataset from a CSV file into a Spark DataFrame.
- Splits the values in some columns of the DataFrame to create arrays using the split function from the **org.apache.spark.sql.functions** package.
- Drops unnecessary columns from the DataFrame using the **drop** function.
- Converts the DataFrame into a Dataset of case class Flight using the as function.
- Performs basic statistics on numerical columns using the **describe** function.
- Aggregates the data by the segmentsCabinCodeArray column to calculate the average base fare and total fare using the **groupBy** and **agg** functions.
- Defines a user-defined function (UDF) to convert the travelDuration column from string format to seconds using the Duration class from the **java.time package**.
- Registers the UDF using the **udf** function from the org.apache.spark.sql.functions package.
- Applies the registered UDF to the travelDuration column using the **withColumn** function.
- Filters out rows with null values in the segmentsAirlineNameArray and travelDurationSeconds columns using the **filter** function.
- Explodes the segmentsAirlineNameArray column to create a new row for each airline using the **explode** function.
- Performs aggregation on the exploded dataset to calculate the average base fare and total fare by airline using the **groupBy** and **agg** functions.
- Displays the results of the analysis using the show function.

## Exploratory Data Analysis
EDA is the process of exploring and analyzing a dataset to gain insights and understand its characteristics. 
It involves tasks such as checking the data structure, generating statistical summaries, visualizing the data through plots and charts, cleaning the data, creating new features, aggregating the data, and identifying correlations between variables. 
EDA helps in identifying patterns, trends, and relationships within the data, and provides a foundation for further analysis and decision-making. 
It is an iterative process that involves examining the data from different perspectives to uncover valuable insights and draw conclusions.
- Load the Dataset: Load the CSV file into a Spark DataFrame or any other suitable data structure for processing in Spark.

- Check Data Structure: Use Spark's DataFrame API to examine the data structure, including the schema, column names, and data types. This step helps to understand the data's structure and identify any potential issues, such as missing or inconsistent data.

- Statistical Summary: Use Spark's DataFrame API to generate basic statistical summary of the dataset, including measures such as mean, median, standard deviation, minimum, maximum, etc. This helps in understanding the central tendency, spread, and distribution of the numerical variables in the dataset.

- Data Cleaning: Perform data cleaning tasks such as handling missing values, correcting inconsistent or erroneous data, and handling outliers, if any. This step ensures that the data is clean and ready for further analysis.

```    
val spark = SparkSession.builder()
.appName("FlightDatasetAnalysis")
.master("local[*]")
.config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
.getOrCreate()
```
The code creates a SparkSession named "FlightDatasetAnalysis" to analyze a flight dataset. It configures Spark to run locally using all available CPU cores, sets a library path for Hadoop, and either gets an existing SparkSession or creates a new one.
```
    val flightDF = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "per" +
        "missive")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\itineraries.csv")
```
The code reads a CSV file named "itineraries.csv" which contains the flight data, located on the desktop of a user's home PC using Spark. It infers the schema of the data, treats the first row as the header, and sets the mode to "permissive" for handling any data parsing errors. The loaded data is stored in a DataFrame named "flightDF".

```
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
```
The code performs multiple transformations on the "flightDF" DataFrame using various functions provided by Spark. It splits certain columns into arrays by splitting the values using the "||" delimiter. It also performs casting of data types for some of the arrays. The resulting DataFrame is named "dfWithSplitValues" and contains several new columns with arrays of values extracted from the original columns in "flightDF".

```
    val cleaneddf = dfWithSplitValues.drop("durationInSecondsArray", "distanceArray", "segmentsDurationInSeconds", "segmentsDistance",
      "segmentsCabinCode", "segmentsEquipmentDescription", "segmentsAirlineCode", "segmentsAirlineName",
      "segmentsDepartureAirportCode", "segmentsArrivalAirportCode", "segmentsArrivalTimeRaw",
      "segmentsArrivalTimeEpochSeconds", "segmentsDepartureTimeRaw", "segmentsDepartureTimeEpochSeconds")
```
This snippet drops several unnecessary columns from the DataFrame named "dfWithSplitValues" using the "drop" method, including "durationInSecondsArray", "distanceArray", "segmentsDurationInSeconds", "segmentsDistance", "segmentsCabinCode", "segmentsEquipmentDescription", "segmentsAirlineCode", "segmentsAirlineName", "segmentsDepartureAirportCode", "segmentsArrivalAirportCode", "segmentsArrivalTimeRaw", "segmentsArrivalTimeEpochSeconds", and "segmentsDepartureTimeRaw", "segmentsDepartureTimeEpochSeconds". The resulting DataFrame named "cleaneddf" is a modified version of "dfWithSplitValues" with these columns removed.


```
case class Flight(legId: String, searchDate: String, flightDate: String, startingAirport: String, destinationAirport: String,
fareBasisCode: String, travelDuration: String, elapsedDays: Int, isBasicEconomy: Boolean,
isRefundable: Boolean, isNonStop: Boolean, baseFare: Double, totalFare: Double, seatsRemaining: Int,
totalTravelDistance: Int, durationInSecondsIntArray: Array[Int], distanceIntArray: Array[Int],
segmentsCabinCodeArray: Array[String], segmentsEquipmentDescriptionArray: Array[String],
segmentsAirlineCodeArray: Array[String], segmentsAirlineNameArray: Array[String],
segmentsDepartureAirportCodeArray: Array[String], segmentsArrivalAirportCodeArray: Array[String],
segmentsArrivalTimeRawArray: Array[String], segmentsArrivalTimeEpochSecondsArray: Array[Long],
segmentsDepartureTimeRawArray: Array[String], segmentsDepartureTimeEpochSecondsArray: Array[Long])
```

```
    val flightsDS = cleaneddf.as[Flight]
```
The code defines a data structure called **Flight** using a case class, which represents flight information with various attributes. Then, it converts a DataFrame named **cleaneddf** into a typed Dataset named "flightsDS" using the **as** method with the **Flight** case class as the type parameter. The resulting "flightsDS" Dataset represents flights data with a schema based on the defined case class, allowing for typed operations and improved type-safety in subsequent data processing.

```
    // Perform basic statistics on numerical columns
    flightsDS.describe("baseFare", "totalFare", "elapsedDays", "seatsRemaining", "totalTravelDistance").show()
```
![Describe](flightsDS_describe.JPG)


