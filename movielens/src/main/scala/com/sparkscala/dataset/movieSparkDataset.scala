package com.sparkscala.dataset

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.sparkscala._

object movieSparkDataset extends App {

  Logger.getRootLogger.setLevel(Level.INFO)

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

  val sc = spark.sparkContext

  import spark.implicits._

  val movie_dataSet = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/movies.csv").as[MovieSchema]

  movie_dataSet.show()

  movie_dataSet.printSchema()

  val rating_dataSet = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/ratings.csv").as[RatingSchema]

  rating_dataSet.show()

  rating_dataSet.printSchema()

  val joined_dataSet = movie_dataSet.joinWith(rating_dataSet, movie_dataSet("movieId") === rating_dataSet("movieId")).
    map { case (a, v) => MovieRatingSchema(a.movieId, a.title, v.userId, v.rating) }

  joined_dataSet.show()
  
  //1. List all the movies and the number of ratings
  
  joined_dataSet.select("movieId", "rating").groupBy("movieId").agg(avg("rating") as ("rating")).orderBy("movieId").show()

  //2. List all the users and the number of ratings they have done for a movie
  
  joined_dataSet.select("movieId", "userId", "rating").groupBy("movieId", "userId").agg(avg("rating") as ("rating")).show

  //3. List all the Movie IDs which have been rated (Movie Id with at least one user rating it)
  
  joined_dataSet.select("movieId", "userId", "rating").filter("rating is not null").show

  //4. List all the Users who have rated the movies (Users who have rated at least one movie)
  
  joined_dataSet.select("movieId", "userId", "rating").filter("rating is not null").show

  //5. List of all the User with the max ,min ,average ratings they have given against any movie
  
  joined_dataSet.select("userId", "rating").groupBy("userId").agg(max("rating") as ("maximum_rating"), min("rating") as ("minimum_rating"), avg("rating") as ("average_rating")).orderBy("userId").show()

  //6. List all the Movies with the max ,min, average ratings given by any user?
  
  joined_dataSet.select("movieId", "rating").groupBy("movieId").agg(max("rating") as ("maximum_rating"), min("rating") as ("minimum_rating"), avg("rating") as ("average_rating")).orderBy("movieId").show()

//7. To find average of each of the ratings that are given for each movie
  
  joined_dataSet.select("movieId", "rating").groupBy("movieId").pivot("rating").avg("rating").withColumnRenamed("avg(rating)", "averageRating").orderBy("movieId").show()
}