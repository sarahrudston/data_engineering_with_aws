# Data Pipelines Project 

## Background
This project is designed to automate the data pipelines of fictional music streaming company Sparkify using Apache Airflow. This is designed to move data from S3 into Sparkify's data warehouse. There are steps for copying and loading the data as well as for additional data quality checks. The source data contains csv logs of user activity in the form of song plays and user data.

### Airflow tasks

The Airflow tasks are linked logically showing the progression of data through the pipeline. Dependencies are set to ensure the DAG completes correctly.

### DAG arguments

The DAG imports the operators from the plugins folder to complete the tasks. The default are args are set so that the DAG has no dependencies on past runs, tasks are retried 3 times, catchup is turned off and there is no email on retry.

### Operators

The operators stage redshift to load the files based on parameters showing where the files reside and also the target tables. The fact and dimension operators run the data transformations to load the data model with an example of a truncate and insert pattern shown. The data quality operator runs checks on the data with expected results.
