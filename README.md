# Project3 Cloud Data Warehouse

## 1. Goal in th3 Project
### 1.1. The purpose of this database in Sparkify
Because Sparkify company want to migrate their system to the cloud. They had put their data in S3 and they want us
help them to build a data warehouse in the cloud and use etl skill to extract their data in S3, then load data to the data warehouse in redshift. And design a schema for analytical goals.

### 1.2 Their analytical goals
They want to use song data and user event data to analyze these users behavior, such as which songs they listen and what time
or how many times they listened. And these song where the location users lived to listened ...etc.

## 2. State and justify my database schema design and ETL pipeline
### 2.1 database schema design
The database schema design code in sql_queryies.py file. And use python to implement create table in create_tables.py file.

#### 2.1.1 Stage schema design
We design two stage table to receive S3 data. staging_events and staging_songs. these table schema have to receive all these data from S3.
1.staging_events: receive data from s3://udacity-dend/log_data
2.staging_songs: receive data from s3://udacity-dend/song_data

#### 2.1.2 dimension tables and fact table schema design
Then design dimension table 
3.users: select users data from staging_events table, be attention to use distinct user_id to prevent duplicate data. 
4.songs: select songs data from staging_songs table, be attention to use distinct song_id to prevent duplicate data.
5.artists: select artists data from staging_songs table, be attention to use distinct artist_id to prevent duplicate data.
6.time: select time data from staging_events table, be attention to use distinct ts to prevent duplicate data.
And fact table
7.songplays: join staging_events and staging_songs by key columns(song and artist column)

### 2.2 ETL pipeline design
The ETL copy S3 data and insert dimension table and fact table SQL code in sql_queryies.py file. And use python to implement these ETL behavior in etl.py file.

log_data input 8056 rows in staging_events table.
song_data input 14896 rows in staging_songs table.
Input 105 rows in users table.
Input 14896 rows in songs table.
Input 10025 rows in artists table.
Input 8023 rows in time table.
Input 333 rows in songplays table

## 3. Provide example queries and results
I had create two jupyter notebook file to implement the project, as follows
1. IaC.ipynb: Use Implement as Code to manage my AWS create/delete redshift and iam role behaviors.
2. execute.ipynb: Use the file to execute create_tables.py and etl.py then provide example queries and results.

In execute.ipynb file, I had run some example about 
1. the etl implement to every table row count.
2. Display dimension table and fact table result.
3. Cube/Slicing/Dicing/Roll-Up/Drill-down query result.
Please read the execute.ipynb result to check my example queries, thanks.