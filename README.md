# Data Modeling with Apache Cassandra

## Purpose

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their 
new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV 
files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer 
the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able 
to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Dataset

There is one dataset called `event_data` which is in a directory of CSV files partitioned by date. 

## Queries

For NoSQL databases, we design the schema based on the queries we know we want to perform. 

We want to ask 3 question of our data:
1. Give me every song in my music library that was listened by men
   - `SELECT song from table_1 where gender = 'M'`
2. Give me song of "Fall Out Boy" that is in my music library that was listened in San Jose-Sunnyvale-Santa Clara
   - `SELECT song from table_2 where artist = 'Fall Out Boy' and location = 'San Jose-Sunnyvale-Santa Clara'`
3. Give me all the artists that was listened in California
   - `SELECT artist from table_3 where state = 'CA'`