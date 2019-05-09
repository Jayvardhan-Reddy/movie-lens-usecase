-- Here we will create temporary table and load the data as part of it

--Table-1

  CREATE EXTERNAL TABLE IF NOT EXISTS temp_movie_ratings (movieid bigint, userid bigint, rating double, time string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
    STORED AS TEXTFILE
    TBLPROPERTIES('skip.header.line.count'='1');


  LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/Movie_Lens/ml-latest-small/ratings.csv' INTO TABLE temp_movie_ratings;
  
  SELECT * FROM temp_movie_ratings LIMIT 10;
  
  --Table-2

  CREATE EXTERNAL TABLE IF NOT EXISTS temp_movie_names (movieid bigint, title string, genres string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
    STORED AS TEXTFILE
    TBLPROPERTIES('skip.header.line.count'='1');

  LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/Movie_Lens/ml-latest-small/movies.csv' INTO TABLE temp_movie_names;
  
  SELECT * FROM temp_movie_names LIMIT 10;
