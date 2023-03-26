import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, corr, count, desc, expr, sum}
import org.apache.spark._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructField, StructType}
object InstaDataEDA {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("DatasetExample")
      .master("local[*]")
      .config("spark.executorEnv.LD_LIBRARY_PATH", "C:\\Program Files\\Hadoop\\bin")
      .getOrCreate()

    import spark.implicits._
    val instagramDataSchema = StructType(Array(
      StructField("profile_id",LongType),
      StructField("sid_profile", LongType),
      StructField("post_id", StringType),
      StructField("location_id", LongType),
      StructField("post_type", IntegerType),
      StructField("numbr_likes", IntegerType),
      StructField("number_comments", IntegerType),
      StructField("profile_name", StringType),
      StructField("firstname_lastname", StringType),
      StructField("following", LongType),
      StructField("followers", LongType),
      StructField("n_posts", IntegerType),
      StructField("url", StringType),
      StructField("is_business_account", BooleanType)
    ))



    val instagramDataFrame = spark.read
      .format("csv")
      .schema(instagramDataSchema)
      //.option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "permissive")
      .option("delimiter", "\t")
      .load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\merged_data.csv\\InstaData.csv")




    // Check the schema of the DataFrame
    instagramDataFrame.printSchema()

    import spark.implicits._
    val instagramPostsSchema = StructType(Array(
      StructField("sid", LongType),
      StructField("sid_profile", LongType),
      StructField("post_id", StringType),
      StructField("profile_id", LongType),
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

    val df1 = spark.read.format("csv").option("header", "true").load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\instagram_posts.csv")
    val df2 = spark.read.format("csv").option("header", "true").load("C:\\Users\\Home PC\\Desktop\\SparkApplication\\instagram_profiles.csv")
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions.col

    def joinDataFrames(df1: DataFrame, df2: DataFrame, joinCol: String): DataFrame = {
      df1.join(df2, col(joinCol), "inner")
    }

    val renamedDF = instagramDF.withColumnRenamed("cts", "df_cts")
      .withColumnRenamed("description", "df_description")
      .withColumnRenamed("sid", "df_sid")

    val renamedPF = instagramPF.withColumnRenamed("cts", "pf_cts")
      .withColumnRenamed("description", "pf_description")
      .withColumnRenamed("sid", "pf_sid")

    val joinedDF = renamedDF.join(renamedPF, Seq("profile_id"), "inner")
    val result = joinedDF.select("profile_id", "df_cts", "df_description", "df_sid", "sid_profile", "post_id", "location_id", "post_type", "numbr_likes", "number_comments", "profile_name", "firstname_lastname", "following", "followers", "n_posts", "url", "pf_cts", "pf_description", "is_business_account")

    result.coalesce(1).write.option("header", "true").csv("merged.csv")
  //  val joinedDF = instagramDF.join(instagramPF, instagramDF("profile_id") === instagramPF("profile_id"), "inner")
    // join the two DataFrames on the 'profile_id' column
    //val mergedDF = instagramDF.join(instagramPF, Seq("profile_id"))

    // drop the duplicate columns
    //val finalDF = mergedDF.drop("sid", "description", "cts")

    // write the final DataFrame to a new csv file
   // finalDF.coalesce(1).write.option("header", "true").csv("merged_instagram_data.csv")

    //joinedDF.show()


    // Stop the SparkSession
    spark.stop()
  }
}