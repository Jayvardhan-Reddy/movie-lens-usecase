--USECASES:

--The following property has to set inorder to perform Join operations
  SET hive.auto.convert.join=false;


1. List all the movies and the number of ratings

soln:
     select hm.title, count(rating) from hive_hbase_movies hm join hive_hbase_rating hr on hm.movieid = hr.movieid group by title limit 10;

2. List all the users and the number of ratings they have done for a movie

soln:
     select userid, count(rating) from hive_hbase_rating group by userid limit 10;

3. List all the Movie IDs which have been rated (Movie Id with at least one user rating it)

soln:
     select hm.movieid, count(hr.rating) as ratedMinOnce from hive_hbase_movies hm join hive_hbase_rating hr on hm.movieid = hr.movieid group by hm.movieid having count(hr.rating) > 1 limit 10;

4. List all the Users who have rated the movies (Users who have rated at least one movie)

soln:
     select hr.userid, count(hr.rating) as ratedMinOneMovie from hive_hbase_rating hr group by hr.userid having count(hr.rating) > 1 limit 10;

5. List of all the User with the max ,min ,average ratings they have given against any movie

soln:
     select hr.userid, max(Distinct hr.rating) as MaxRat, min(Distinct hr.rating) as MinRat, avg(Distinct hr.rating) as AvgRat from hive_hbase_rating hr group by hr.userid limit 10; 

6. List all the Movies with the max ,min, average ratings given by any user

soln:
     select hm.movieid, hm.title, max(Distinct hr.rating) as MaxRat, min(Distinct hr.rating) as MinRat, avg(Distinct hr.rating) as AvgRat from hive_hbase_movies hm join hive_hbase_rating hr on (hm.movieid = hr.movieid) group by hm.movieid, hm.title limit 10;
