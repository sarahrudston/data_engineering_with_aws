# Sparkify ETL project

This project is designed to provide an ETL pipeline for fictional music streaming startup Sparkify. This mimics a process of transferring a user and song database into storage and processes which reside in the cloud. 

The source data (along with its metadata) can be found in S3. The data is comprised of JSON logs of user activity on the Sparkify app. The new ELT pipeline extracts the data, stages it into intermediary tables and then moves it into fact and dimension tables. These can be used to help the company find insights of what their customers are listening to.

## Source data

The log files can be found at s3://udacity-dend/log_data. The data is partitioned by year and month. The song data can be found at s3://udacity-dend/song_data, partitioned by the first three letters of the track ID.

## Data model

The majority of insights can be gleaned from the songplays fact table which contains data related to song plays (events). This data can then be augmented by data from the dimension tables. You can see a UML diagram of the data model in the schema.png file.

## Creating the tables and loading the data

The etl.py file provides the Python needed to create the tables and copy data into them. The SQL scripts to create the appropriate tables can be found in sql_queries.py. You can also see a Python notebook called Test Connection which I used to run the script and create the tables to test.

## Additional queries

### Most popular songs by artist and songplay count:

select s.title as song, a.name as artist, count(*) as number_of_plays from songplays sp left join songs s on sp.song_id=s.song_id left join artists a on a.artist_id=s.artist_id where sp.song_id is not null group by s.title, a.name order by number_of_plays desc

### Most popular artist by hour of the day and songplay count
select t.hour, a.name as artist, count(*) as number_of_plays from songplays sp left join artists a on a.artist_id=sp.artist_id left join time t on t.start_time = sp.start_time where sp.song_id is not null and t.hour is not null group by t.hour, a.name order by number_of_plays desc

### Most popular artist by location and songplay count
select a.location as location, a.name as artist, count(*) as number_of_plays from songplays sp left join artists a on a.artist_id=sp.artist_id where sp.song_id is not null and a.location is not null and a.location !='' group by a.location, a.name order by number_of_plays desc
