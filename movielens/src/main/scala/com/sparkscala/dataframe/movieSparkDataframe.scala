package com.sparkscala.dataframe

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object movieSparkDataframe extends App {

  Logger.getRootLogger.setLevel(Level.INFO)

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

  val sc = spark.sparkContext

  val movie_dataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/movies.csv")

  movie_dataFrame.show()
  
  movie_dataFrame.printSchema()
  
  val rating_dataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/ratings.csv")

  rating_dataFrame.show()
  
  rating_dataFrame.printSchema()

  val join_movieRating_df = movie_dataFrame.join(rating_dataFrame, "movieId").cache()
  
  join_movieRating_df.show()

  //1. List all the movies and the number of ratings
  
  join_movieRating_df.select("movieId", "rating").groupBy("movieId").agg(avg("rating") as ("rating")).orderBy("movieId").show()

  //2. List all the users and the number of ratings they have done for a movie
  
  join_movieRating_df.select("movieId", "userId", "rating").groupBy("movieId", "userId").agg(avg("rating") as ("rating")).show

  //3. List all the Movie IDs which have been rated (Movie Id with at least one user rating it)
  
  join_movieRating_df.select("movieId", "userId", "rating").filter("rating is not null").show

  //4. List all the Users who have rated the movies (Users who have rated at least one movie)
  
  join_movieRating_df.select("movieId", "userId", "rating").filter("rating is not null").show

  //5. List of all the User with the max ,min ,average ratings they have given against any movie
  
  join_movieRating_df.select("userId", "rating").groupBy("userId").agg(max("rating") as ("maximum_rating"), min("rating") as ("minimum_rating"), avg("rating") as ("average_rating")).orderBy("userId").show()

  //6. List all the Movies with the max ,min, average ratings given by any user?
  
  join_movieRating_df.select("movieId", "rating").groupBy("movieId").agg(max("rating") as ("maximum_rating"), min("rating") as ("minimum_rating"), avg("rating") as ("average_rating")).orderBy("movieId").show()

  // dataframe.write().mode(SaveMode.Overwrite).save("C:\\codebase\\scala-project\\inputdata\\output\\data");
  // dataframe.write().format("json").save("C:\\codebase\\scala-project\\inputdata\\output\\data");
}