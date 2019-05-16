package com.sparkscala

case class MovieSchema(movieId: Int, title: String, genres: String)

case class RatingSchema(userId: Int, movieId: Int, rating: Double, timestamp: String)

case class MovieRatingSchema(movieId: Int, title: String, userId: Int, rating: Double)