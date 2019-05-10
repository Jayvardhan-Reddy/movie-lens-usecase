--ACTUAL TABLES
--Hive & HBase tables are created simultaneously by using HBaseStorageHandler i.e, Hive-Hbase integration

--Table-1

  CREATE TABLE hive_hbase_rating (time string, movieid bigint, userid bigint,rating double) 
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
    WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,r:movieid,r:userid,r:rating") 
    TBLPROPERTIES ("hbase.table.name" = "hb_rating");

  INSERT INTO hive_hbase_rating SELECT time,movieid,userid,rating FROM temp_movie_ratings;
    
  SELECT * FROM hive_hbase_rating LIMIT 10;
    
--Table-2

  CREATE TABLE hive_hbase_movies (movieid bigint, title string, genres string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,m:title,m:genres") 
  TBLPROPERTIES ("hbase.table.name" = "hb_movies");

  INSERT INTO hive_hbase_movies SELECT * FROM temp_movie_names;

  SELECT * FROM hive_hbase_movies LIMIT 10;
