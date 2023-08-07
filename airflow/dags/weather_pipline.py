from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import sys
load_dotenv()
# WORK_DIR = os.environ['WORK_DIR'] 
# WORK_DIR = 'D:\projects\\real_time_weather_predict\\airflow' # for windows system
WORK_DIR = '/opt/airflow' # for docker
sys.path.append(f'/opt/airflow')
from helper.weather_class import WeatherPipeline
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{WORK_DIR}/config/ServiceKey_GoogleCloud.json'

locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
weather = WeatherPipeline(locations)

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='load_weather_data',
    default_args=default_args,
    start_date=datetime(2023, 7, 20),
    schedule_interval= '@hourly',
    catchup=False
) as dag:
    # 1- extract data
    @task
    def extract_data():
        weather.extract_data()
    
    # 2- load data to cloud storage
    @task
    def load_data_to_cloudStorage():
        weather.load_to_cloud()
    
    # 3- load to bigquery
    @task
    def load_to_bigquery(dataset_name, table_name):
        df = weather.process_data()
        weather.load_data_to_bigquery(df, dataset_name= dataset_name, table_name=table_name)

    # run DAG
    extract_data() >> load_data_to_cloudStorage() >> load_to_bigquery('weather', 'weather')