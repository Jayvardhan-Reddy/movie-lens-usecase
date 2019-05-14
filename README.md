# Movie-Lens-Usecase
Project to determine the ratings of a movie using the movie-lens dataset.

#### Dataset downlod link
Download ml-latest-small.zip dataset @ http://grouplens.org/datasets/movielens/

#### Technology stack used individually to solve the below usecases.

- Hive
- Hbase
- Spark
- MapReduce

#### Usecases:

1.	List all the movies and the number of ratings
2.	List all the users and the number of ratings they have done for a movie
3.	List all the Movie IDs which have been rated (Movie Id with at least one user rating it)
4.	List all the Users who have rated the movies (Users who have rated at least one movie)
5.	List of all the User with the max ,min ,average ratings they have given against any movie
6.	List all the Movies with the max ,min, average ratings given by any user

#### Solution:

- Hive-HBase Integration
- Spark (RDD, Dataframe, DataSet) solving individually for each category
- Mapreduce
