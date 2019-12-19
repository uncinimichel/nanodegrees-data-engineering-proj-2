# Data Modeling with Postgres
  
In this project I am creating a Postgres database called sparkifydb with the following tables:

Fact table:

`songplays` - records in log data associated with song plays NextSong

Dimension Tables:

`users` - users in the app

`songs` - songs in music database

`artists` - artists in music database

`time`- timestamps of records in songplays broken down into specific units

Part of the project is also to populate the above tables.

 
So then they can correlate songs with users activity. For example you can query songs that users are listening to.
    
## Files in the repo

- data

contain the [Dataset]

- create_tables.py

drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts

- sql_queries.py

contains all your sql queries 

- etl.py

reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook


- There are two jupiter notebook used for experiments with the data:

    - etl.ipynb

    reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables

    - test.ipynb

    displays the first few rows of each table to let you check your database



## How to run the project

### set up the database schema
`python create_tables.py`

### populate the database
`python etl.py`
 
 
## Dataset 

We are using two Dataset:

- data/song_data

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```


- data/log_data

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
```
{"artist":Mynt,"auth":"Logged In","firstName":"Celeste","gender":"F","itemInSession":1,"lastName":"Williams","length":null,"level":"free","location":"Klamath Falls, OR","method":"GET","page":"Home","registration":1541077528796.0,"sessionId":52,"song":null,"status":200,"ts":1541207123796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.103 Safari\/537.36\"","userId":"53"}
```


