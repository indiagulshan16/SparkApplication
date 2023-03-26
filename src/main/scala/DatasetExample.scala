import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, corr, count, desc, expr, sum}
import org.apache.spark._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructField, StructType}
object DatasetExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("DatasetExample")
      .master("local[*]")
      .config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
      .getOrCreate()

    val inputPath = "C:\\Users\\Home PC\\Desktop\\SparkApplication\\instagram_posts.csv"
    // Read in the dataset as a DataFrame
//    val instagramDF = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv(inputPath)
//    val instagramDF = spark.read
//  .option("header", "true")
//  .option("delimiter", "\t")
//  .option("charset", "ISO-8859-1") // or the encoding used in your CSV file
//  .csv(inputPath)


    import spark.implicits._
    val instagramPostsSchema = StructType(Array(
      StructField("sid", LongType),
      StructField("sid_profile", LongType),
      StructField("post_id", StringType),
      StructField("profile_id",LongType),
      StructField("location_id", LongType),
      StructField("cts", DateType),
      StructField("post_type", IntegerType),
      StructField("description", StringType),
      StructField("numbr_likes", IntegerType),
      StructField("number_comments", IntegerType)
    ))
    val instagramProfilesSchema = StructType(Array(
      StructField("sid", LongType),
      StructField("profile_id", LongType),
      StructField("profile_name", StringType),
      StructField("firstname_lastname", StringType),
      StructField("description", StringType),
      StructField("following", LongType),
      StructField("followers", LongType),
      StructField("n_posts", IntegerType),
      StructField("url", StringType),
      StructField("cts", DateType),
      StructField("is_business_account", BooleanType)
    ))
//    case class InstagramData(sid: String, sid_profile: String, post_id: String, profile_id: String, location_id: String,
//                             cts: String, post_type: String, description: String, number_likes: Int, number_comments: Int)
//
//    case class InstagramProfileData(sid: String, profile_id: String, profile_name: String, firstname_lastname: String,
//                                    description: String, following: Int, followers: Int, n_posts: Int, url: String,
//                                    cts: String, is_business_account: Boolean)
//    implicit val instagramProfileDataEncoder = Encoders.product[InstagramProfileData]
//    implicit val instagramDataEncoder = Encoders.product[InstagramData]


    val instagramDF = spark.read
      .format("csv")
      .schema(instagramPostsSchema)
      //.option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "permissive")
      .option("delimiter", "\t")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\instagram_posts.csv")


    val instagramPF = spark.read
      .format("csv")
      .schema(instagramProfilesSchema)
      //.option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "permissive")
      .option("delimiter", "\t")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\instagram_profiles.csv")


    // Check the schema of the DataFrame
    instagramDF.printSchema()
    instagramPF.printSchema()

val joinedDF = instagramDF.join(instagramPF, Seq("profile_id"), "inner").drop("cts", "sid","description")
    //val joinedDF = instagramDF.join(instagramPF.select("profile_id", "sid_profile"), Seq("profile_id"), "inner")

    //val resultDF = joinedDF.groupBy("sid", "sid_profile", "post_id", "profile_id", "location_id", "cts", "post_type", "description", "numbr_likes", "number_comments", "profile_name", "firstname_lastname", "description", "following", "followers", "n_posts", "url", "cts", "is_business_account")
     // .agg(avg("numbr_likes").as("avg_likes"), avg("number_comments").as("avg_comments"))

    //resultDF.show(5)
    //  .groupBy(instagramPF("profile_name"))
//  .agg(avg(instagramDF("numbr_likes")).as("avg_likes"), avg(instagramDF("number_comments")).as("avg_comments"))

     // resultDF.write.mode("overwrite").option("header", "true").csv("merged_data.csv")
    joinedDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("merged_data.csv")


//    val joinedDF = instagramPF.join(instagramDF, Seq("profile_id"), "inner")
//  .select("*")
//    joinedDF.write
//      .format("csv")
//      .option("header", true)
//      .mode("overwrite")
//      .save("path/to/new/file.csv")

    // Explore basic statistics of numeric columns
//    val likesStats = instagramDF.select("numbr_likes", "number_comments").describe()
//    likesStats.show()

//    // Explore missing values
//    val missingValues = instagramDF.select(
//      instagramDF.columns.map(colName => functions.sum(col(colName).isNull.cast("int")).alias(colName)): _*
//    )
//    missingValues.show()
//
    // Explore distribution of likes and comments
//    val likesDist = instagramDF.select("numbr_likes", "number_comments")
//      .withColumn("likes_buckets", expr("case when numbr_likes < 10 then '<10' else '>10' end"))
//      .groupBy("likes_buckets")
//      .agg(avg("numbr_likes").alias("avg_likes"), avg("number_comments").alias("avg_comments"))
//    likesDist.show()
//
//    // Explore relationship between likes and comments
//    val likesCommentsCorr = instagramDF.select("numbr_likes", "number_comments")
//      .stat.corr("numbr_likes", "number_comments")
//    println(s"Pearson correlation between likes and comments: $likesCommentsCorr")
//
    // Explore top users with most posts and likes
//    val topUsers = instagramDF.groupBy("profile_id")
//      .agg(count("*").alias("num_posts"), sum("numbr_likes").alias("total_likes"))
//      .orderBy(desc("num_posts"), desc("total_likes"))
//    topUsers.show()
//
//    // Explore top posts with most likes and comments
//    val topPosts = instagramDF.select("post_id", "numbr_likes", "number_comments")
//      .orderBy(desc("numbr_likes"), desc("number_comments"))
//    topPosts.show()






//    // Check for missing values
//    df.na.drop().show()
//
//    // Compute summary statistics
//    df.select("numbr_likes", "number_comments").summary().show()
//
//    // Visualize the distribution of the data
//    df.select("numbr_likes", "number_comments")
//      .groupBy("numbr_likes", "number_comments")
//      .count()
//      .orderBy(desc("count"))
//      .show()
//
//    // Compute correlations
//    println("Correlation between numbr_likes and number_comments:")
//    df.select(corr("numbr_likes", "number_comments")).show()
//
//    // Visualize the data over time
//    df.select("cts", "numbr_likes", "number_comments")
//      .groupBy("cts")
//      .agg(avg("numbr_likes"), avg("number_comments"))
//      .orderBy("cts")
//      .show()
//
//    // Analyze the text data
//    df.select("description")
//      .filter(col("description").isNotNull)
//      .show(false)



    // Stop the SparkSession
    spark.stop()
  }
}