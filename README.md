# ETL_toll_data_Pipeline

## Scenario

You are a data engineer at a data analytics consulting company. You have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. This project consists of the following two main parts:

### First Part

In the first part of this project, our job is to create a data pipeline that collects the streamed data fro toll plaza systems and loads it into a database using Apache Kafka.

**Note**: As a vehicle passes a toll plaza, the vehicle's data like vehicle_id, vehicle_type, toll_plaza_id, and timestamp are streamed to Kafka.

**Part One Tasks:**

* Start a MySQL Database server.
* Create a table to hold the toll data.
* Start the Kafka server.
* Install the Kafka Python driver.
* Install the MySQL Python driver.
* Create a Kafka topic named toll.
* Create a customized generator program to stream data to the toll topic.
* Create a customized consumer program to write data into a MySQL database table.
* Verify that streamed data is being collected in the database table.

### Second Part

In the second part of this project, our job is to collect data available in different formats and consolidate it into a single file using Apache Airflow.

**Note**: Each highway is operated by a different toll operator with different IT setups that use different file formats.

**Part Two Tasks:**

* Define DAG arguments.
* Define the DAG.
* Create a task to download data.
* Create a task to extract data from a CSV file.
* Create a task to extract data from a TSV file.
* Create a task to extract data from a fixed-width file.
* Create a task to consolidate data extracted from previous tasks.
* Transform the data.
* Define the task pipeline.
* Monitor the DAG.

