package com.sparkscala.core
import com.sparkscala._

object movieSparkCore extends App {

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

  val sc = spark.sparkContext

  //movie dataset

  println("movie dataset!!!")

  val movie_rdd_withHeader = sc.textFile("src/main/resources/movies.csv")

  val movie_header = movie_rdd_withHeader.first()

  println("movie header ====>" + movie_header)

  val movie_rdd = movie_rdd_withHeader.filter(row => row != movie_header).map(_.split(","))

  println("Sample Data ====================> " + movie_rdd.take(1).foreach(l => println(l.mkString(","))))

  val movie_rdd_map = movie_rdd.map(ele => (ele(0).toInt, MovieSchema(
    ele(0).toInt, ele(1).toString(), ele(2).toString())))

  println("Sample Movie MAP Data ====================>" + movie_rdd_map.take(10).mkString)

  //ratings dataset

  println("ratings dataset!!!")

  val ratings_rdd_withHeader = sc.textFile("src/main/resources/ratings.csv")

  val ratings_header = ratings_rdd_withHeader.first()

  println("ratings header ====>" + ratings_header)

  val ratings_rdd = ratings_rdd_withHeader.filter(row => row != ratings_header).map(_.split(","))

  println("Sample Rating Map Data ====================> " + ratings_rdd.take(1).foreach(l => println(l.mkString(","))))

  val ratings_rdd_map = ratings_rdd.map(ele => (ele(1).toInt, RatingSchema(
    ele(0).toInt, ele(1).toInt, ele(2).toFloat, ele(3).toString())))

  val join_movieRating = movie_rdd_map.join(ratings_rdd_map)

  println("Joined Data ====================> " + join_movieRating.take(1).foreach(l => println(l._1, l._2)))

  //------------------------------------------------------- Usecase Begins ------------------------------------------------------------

  // 1.	List all the movies and the number of ratings

  val movie_numRatings = join_movieRating.map(ele => (ele._1, (ele._2._1.title, ele._2._2.rating)))

  println("movie_numRatings Data ====================> " + movie_numRatings.take(10).foreach(l => println(l._1, l._2)))

  //val key_movie_numRatings = movie_numRatings.map(ele => ((ele._1, ele._2._1),ele._2._2))

  val key_movie_numRatings = movie_numRatings.map(ele => (ele._2._1, ele._2._2))

  val final_movie_numRatings = key_movie_numRatings.countByKey()

  println("All the movies and the number of ratings ====================> " + final_movie_numRatings.mkString)

  // 2.	List all the users and the number of ratings they have done for a movie

  // val user_numRatings = join_movieRating.map(ele => (ele._2._2.userId, ele._2._2.rating)).countByKey()

  // or

  val user_numRatings = ratings_rdd_map.map(ele => (ele._2.userId, ele._2.rating)).countByKey()

  println("All the User and the number of ratings ====================> " + user_numRatings.mkString)

  // 3.	List all the Movie IDs which have been rated (Movie Id with at least one user rating it) 

  val movieUser_Ratings = join_movieRating.map(ele => (ele._1, ele._2._2.rating)).countByValue().filter(ele => ele._2 != null)

  println("Movie Id with at least one user rating it ====================> " + movieUser_Ratings.mkString)

  // 4.	List all the Users who have rated the movies (Users who have rated at least one movie)

  val userRated_Movies = ratings_rdd_map.map(ele => (ele._2.userId, ele._2.rating)).countByValue().filter(ele => ele._2 > 0)

  println("Users who have rated at least one movie ====================> " + userRated_Movies.mkString)

  // 5.	List of all the User with the max ,min ,average ratings they have given against any movie

  val userMinMaxAvg_Ratings = join_movieRating.map(ele => (ele._2._2.userId, ele._2._2.rating))

  val grouped_UserMinMaxAvg_Ratings = userMinMaxAvg_Ratings.groupByKey()

  val final_UserMinMaxAvg_Ratings = grouped_UserMinMaxAvg_Ratings.map(x => {
    val userid = x._1
    val max_rating = x._2.max
    val min_rating = x._2.min
    val rating_sum = x._2.sum
    val rating_count = x._2.size
    val avg = rating_sum / rating_count
    ("   userid :   " + userid + "    max_rating: " + max_rating + "    min_rating:  " + min_rating + "    average_rating:  " + avg)
  })

  println(final_UserMinMaxAvg_Ratings.foreach(x => println(x)))

  // 6. List all the Movies with the max ,min, average ratings given by any user?

  val movieMinMaxAvg_Ratings = join_movieRating.map(ele => (ele._2._1.movieId, ele._2._2.rating))

  val grouped_MovieMinMaxAvg_Ratings = movieMinMaxAvg_Ratings.groupByKey()

  val final_MovieMinMaxAvg_Ratings = grouped_MovieMinMaxAvg_Ratings.map(x => {
    val movieid = x._1
    val max_rating = x._2.max
    val min_rating = x._2.min
    val rating_sum = x._2.sum
    val rating_count = x._2.size
    val avg = rating_sum / rating_count
    ("   movieid :   " + movieid + "    max_rating: " + max_rating + "    min_rating:  " + min_rating + "    average_rating:  " + avg)
  })

  println(final_MovieMinMaxAvg_Ratings.foreach(x => println(x)))

}