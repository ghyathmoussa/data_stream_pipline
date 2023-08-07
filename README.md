# Data Pipeline 
This application create an automation system for weather prediction using airflow, Bigquery and flask API
## Workflow
1. The application get the data from [rapidapi](https://rapidapi.com) 
2. Preprocess data 
3. Create pipeline to add data to airflow
4. create pipeline to upload data to Bigquery in Google Cloud

## **Installation**
* Install airflow in your system

* Install Docker then use the command ```docker-compose up``` to build the docker image

* setup you Google Cloud system
* download the configuration json file that helps you to connect to BigQuery

