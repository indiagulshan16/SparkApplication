# SparkApplication
#InstaDataEDA
This is a scala code that reads Instagram data from CSV files and performs exploratory data analysis(EDA) using Apache Spark library.
The code allows users to join two CSV files, performs various data cleaning operations and generate insights using SparkSQL.
To use this code, you will need to install the following dependencies:

- Apache Spark
- Scala

The code reads two CSV files: `instagram_posts.csv` and `instagram_profiles.csv`. The files should contain the following columns:

- `instagram_posts.csv`: sid, sid_profile, post_id, profile_id, location_id, cts, post_type, description, numbr_likes, number_comments
- `instagram_profiles.csv`: sid, profile_id, profile_name, firstname_lastname, description, following, followers, n_posts, url, cts, is_business_account

To run the code, simply execute the `InstaDataEDA.scala` file in your Scala IDE or using the `spark-submit` command.

## Examples

Here are some examples of how to use the code:

- To join the two CSV files based on the `profile_id` column:
val joinedDF = joinDataFrames(instagramDF, instagramPF, "profile_id")

- To generate a summary of the Instagram data:
val summaryDF = joinedDF.select(avg("numbr_likes"), avg("number_comments"), count("post_id"))
summaryDF.show()


- To filter out all rows where the `numbr_likes` column is less than 100:
val filteredDF = joinedDF.filter(col("numbr_likes") > 100)
filteredDF.show()
