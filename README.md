# Project: STEDI Human Balance Analytics

## Project Instructions

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

## Requirements

To simulate the data coming from the various sources, you will need to create your own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point.

You have decided you want to get a feel for the data you are dealing with in a semi-structured format, so you decide to create two Glue tables for the two landing zones. Share your customer_landing.sql and your accelerometer_landing.sql script in git.
Query those tables using Athena, and take a screenshot of each one showing the resulting data. Name the screenshots customer_landing(.png,.jpeg, etc.) and accelerometer_landing(.png,.jpeg, etc.).
The Data Science team has done some preliminary data analysis and determined that the Accelerometer Records each match one of the Customer Records. They would like you to create 2 AWS Glue Jobs that do the following:

Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.
You need to verify your Glue job is successful and only contains Customer Records from people who agreed to share their data. Query your Glue customer_trusted table with Athena and take a screenshot of the data. Name the screenshot customer_trusted(.png,.jpeg, etc.).
Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to.

The Data Science team would like you to write a Glue job that does the following:

Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
Finally, you need to create two Glue Studio jobs that do the following tasks:

Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.

## Task List

- [x] A Python script using Spark that sanitizes the Customer data from the Website (Landing Zone) and only stores the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

* [customer_landing_to_trusted.py](./scripts/customer_landing_to_trusted.py)
* [customer_trusted.sql](./scripts/customer_trusted.sql)

- [x] A Python script using Spark that sanitizes the Accelerometer data from the Mobile App (Landing Zone) - and only stores Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted

* [accelerometer_landing_to_trusted_zone.py](./scripts/accelerometer_landing_to_trusted_zone.py)
* [accelerometer_trusted.sql](./scripts/accelerometer_trusted.sql)

- [x] A Python script using Spark that sanitizes the Customer data (Trusted Zone) and creates a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.

* [customer_trusted_to_curated.py](./scripts/customer_trusted_to_curated.py)
* [customer_curated.sql](./scripts/customer_curated.sql)

- [x] A Python script using Spark that reads the Step Trainer IoT data stream (S3) and populates a Trusted Zone Glue Table called step_trainer_trusted containing the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

* [step_trainer_landing_to_trusted.py](./scripts/step_trainer_landing_to_trusted.py)
* [step_trainer_trusted.sql](./scripts/step_trainer_trusted.sql)

- [x] A Python script using Spark that creates an aggregated table that has each of the Step Trainer readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and populates a glue table called machine_learning_curated.

* [trainer_trusted_to_curated.py](./scripts/trainer_trusted_to_curated.py)
* [machine_learning_curated.sql](./scripts/machine_learning_curated.sql)

- [x] customer_landing.sql and your accelerometer_landing.sql script along with screenshots customer_landing (.png,.jpeg, etc.) and accelerometer_landing (.png,.jpeg, etc.)

  _**customer_landing.sql and accelerometer_landing.sql script:**_

  - [customer_landing.sql](./scripts/customer_landing.sql)
  - [accelerometer_landing.sql](./scripts/accelerometer_landing.sql)

  _**Screenshot**_

  - `customer_landing` table:

      <img src="./images/customer_landing.png">

  - `accelerometer_landing` table:

      <img src="./images/accelerometer_landing.png">

- [x] A screenshot of your Athena query of the customers_trusted Glue table called customer_trusted(.png,.jpeg, etc.)

  _**Screenshot**_

  - `customer_trusted` table:

      <img src="./images/customer_trusted.png">

      <img src="./images/customer_trusted_check.png">
