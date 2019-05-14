--HBASE TABLE
--This is added to show an alternative way of creating and importing data into Hbase

--To directly import the CSV file into HBase table from normal prompt([cloudera@quickstart ~]$) and not from Hbase shell.

  create 'mov','m'

  hdfs dfs -copyFromLocal '/home/cloudera/Desktop/Movie_Lens/ml-latest-small/movies.csv' '/user/jay/'

  hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=','  -Dimporttsv.columns=HBASE_ROW_KEY,movie_rating:movieid,movie_rating:title,movie_rating:genres mov_demo hdfs://quickstart.cloudera/user/jay/movies.csv
