---
title: 'Data lake on AWS'
---

Data lake on AWS
===

## Table of Contents

[TOC]

## Overall

That is the brief of the project

* That is the ETL pipeline that extracts data from S3, and then stages them in Redshift, finally transforms data into a set of dimensional tbales for analytics.

* Sparkify is the star schema with fact table of songplays and dimension tables of songs, users, artists, and time.

* Analytic goals
    * With this model, the analyst can query to extract the information about how many times the song played. 
    * It can provide the information about user preferences. 
    * The artists can know what their song is listened the most.
    * The data team can use it to create a machine learning model for recommendation system.   
    
## How to run:
* Install python, jupyter notebook, and dependencies (Pyspark, Pandas, Numpy...)
* Run etl.py
* Run cells in test.ipynd to see the result

## File explainations:

| File              | Explaination            |
| ----------------- |:----------------------- |
| elt.py | This is a pipeline script to extract data from data S3 to store in S3 for data lake, process data and load to dimensional database and repartition data for optimize spark jobs  |
| analytics.ipynd   | This notebook to test data from output and check the quality of data and write analytical queries as well |

Data description
---
* Songplays table: 
    ```sql=
    root
     |-- user_id: string (nullable = true)
     |-- ts: long (nullable = true)
     |-- level: string (nullable = true)
     |-- song_id: string (nullable = true)
     |-- artist_id: string (nullable = true)
     |-- session_id: long (nullable = true)
     |-- location: string (nullable = true)
     |-- user_agent: string (nullable = true)
     |-- year: integer (nullable = true)
     |-- month: integer (nullable = true)
    ```
    > That is the fact table. That contains logs data of music playing application. 
    > Those parket files are partitioned by year and month. Also they are stored in S3 in directory S3://udacity-longnpp/sparkify/songplays_table


* Users table: 
    ```sql=
    root
     |-- user_id: string (nullable = true)
     |-- first_name: string (nullable = true)
     |-- last_name: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- level: string (nullable = true)
    ```
    > That is the dimension table. That contains user data of music playing application
    > This table is partitioned with number of score. 

* Songs table: 
    ```sql=
    root
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- year: integer (nullable = true)
     |-- artist_id: string (nullable = true)
    ```
    > That is the dimension table. That contains song data of music playing application
    > This parquet files are partitioned by year and month and stored in s3://udacity-longnpp/sparkify/song_table

* Artists table: 
    ```sql=
    root
     |-- artist_id: string (nullable = true)
     |-- name: string (nullable = true)
     |-- location: string (nullable = true)
     |-- latitude: double (nullable = true)
     |-- longitude: double (nullable = true)
    ```
    > That is the dimension table. That contains artists data of music playing application
    > This table is stored in s3://udacity-longnpp/sparkify/artist_table with parquet format

* Time table: 
    ```sql=
    root
     |-- ts: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- date: date (nullable = true)
     |-- hour: integer (nullable = true)
     |-- day: integer (nullable = true)
     |-- week: integer (nullable = true)
     |-- weekday: integer (nullable = true)
     |-- year: integer (nullable = true)
     |-- month: integer (nullable = true)
    ```
    > That is the dimension table. That contains time data of logs data.
    > Those parquet files are stored in s3://udacity-longnpp/sparkify/time_table


Analytics
---
* Count number of users per week
```python=
songplays_table.join(time_table, 
                     on=[songplays_table.ts == time_table.ts], 
                     how="left")\
                    .groupBy("week")\
                    .agg(F.countDistinct("user_id").alias("count_user"))\
                    .show()
```

* Count number of song plays of gender and level users
```python=
songplays_table.join(user_table, 
                    on=[songplays_table.user_id == user_table.user_id],
                    how="left")\
                .groupBy("gender",user_table.level)\
                .count().alias("number_songplays")\
                .show()
```
* Count number of users with pivot table of location and weeks
```python=
songplays_table.join(time_table, 
                     on=[songplays_table.ts == time_table.ts], 
                     how="left")\
                .groupBy("location")\
                .pivot("week")\
                .agg(F.countDistinct("user_id").alias("number_user"))\
                .show()
```

## Appendix and FAQ

:::info
**Please assess my project?** Leave a comment!
:::

###### tags: `Udacity` `Data Modelling` `S3` `Redshift` `Pipeline`
