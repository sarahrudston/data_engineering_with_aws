## STEDI Human Balance Analytics

# Background

This project is to process and transform data for a fictional company which has created a Step Trainer and wants to use the data collected to train a machine learning model. The company - STEDI - has a piece of hardware that trains the user to do a balance exercise, uses sensors on the app to detect steps and then collects customer data from a linked mobile app.

The motion sensor records the distance of the object detected and the app uses an accelerometer to detect motion in 3 directions - x, y and z. The team now wants to use the motion sensor data to train a machine learning model to detect steps in real-time. 

# Considerations 

Some early adopters have agreed to share their data for research purposes and therefore only their data should be included in the final models. The landing data contains user data which is not allowed for use in the final models.

We have three main data sources - customer records, step trainer records and accelerometer records. They are all in JSON format and can be loaded to S3 buckets.

# Structure

S3 buckets were set up in 3 partitions for customer, step trainer and accelerometer data. These were then partitioned again by landing, trusted and curated data. We also have an additional space for curated machine learning data. This is an aggregated table for each step trainer reading and associated accelerometer reading for customers who have agreed to share their data.

The scripts created by the Glue studio jobs detail the source data, joins, transformations and targets for each data source. You can also see the structure of the landing tables in the DDL scripts. The screenshots folder contains the correct count of records for each table. There is an additional screenshot for a new version of the accelerator_trusted job which removes any records which were taken before the customer gave consent for their data to be used for research.
