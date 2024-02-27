# Data Pipeline 

## Overview

This repository contains a project of Python scripts and Airflow DAG (Directed Acyclic Graph) designed to extract data from Brasil.IO API, transform it, and load it into a PostgreSQL database or stream the data to Kafka. The data extracted pertains to parliamentary expenses.

## Project Structure

**dags/: Directory containing Airflow DAG definition file.**
**dags/answer_questions.py: Python script responsible for fetching data from Brasil.IO API, transforming it, and loading it into the PostgreSQL database.**
**dags/kafka_stream.py: Python script responsible for streaming data from Brasil.IO API into the Kafka topics.**
**sql/: Directory containing SQL files used by the Airflow DAG.**
**sql/create_table_db.sql: SQL script defining the table structure used to store the extracted data.**
**docker-compose.yml: Central configuration file for defining and managing multi-container Docker applications, simplifying the process of development, testing, and deployment.**
**requirements.txt: File with the name of libraries required to run with docker-compose.**
**spark_stream.py: Python script responsible for consume data from Kafka topics and insert in Cassandra using spark.**

## Acknowledgments

This project was created as a part of a data engineering task of challenge of Havan Labs.
Thanks to Brasil.IO for providing access to the parliamentary expense data through their API.
