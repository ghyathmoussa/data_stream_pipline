import requests
import json
import datetime
import os
from dotenv import load_dotenv
from google.cloud.storage.client import Client as sc
from google.cloud.bigquery.client import Client as bc
from google.cloud.exceptions import Conflict
import pandas as pd
from xgboost import XGBRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split

# Global variables

load_dotenv('/opt/airflow/.env')
# WORK_DIR = os.getenv('WORK_DIR')
# WORK_DIR = '/mnt/d/projects/real_time_weather_predict'
# WORK_DIR = 'D:\projects\\real_time_weather_predict'
WORK_DIR = '/opt'
# DATA_DIR = os.getenv('DATA_DIR')
# DATA_DIR = 'D:\projects\\real_time_weather_predict\\airflow\data'
DATA_DIR = '/opt/airflow/data'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{WORK_DIR}/airflow/config/ServiceKey_GoogleCloud.json'

STORAGE_CLIENT = sc()
BIGQUERY_CLIENT = bc()
CURRENT_DATE = datetime.datetime.now().strftime('%Y-%m-%d').replace('-','_')

# weather class

class WeatherPipeline:
    def __init__(self, locations = None):
        self.locations = locations
    
    def extract_data(self):
        # extract data from API
        # store the data as txt file in data directory

        if not os.path.exists(f'{WORK_DIR}/airflow/data/{CURRENT_DATE}'):
            print('File ')
            os.mkdir(f'{WORK_DIR}/airflow/data/{CURRENT_DATE}') # folder for data in current date
        
        # get the weather from each location
        for loc in self.locations:
            URL = f'https://weatherapi-com.p.rapidapi.com/current.json?q={loc}&lang=en'

            headers = {
                "X-RapidAPI-Key": "24cc538b51msh9dd38f0d1f4fd7ap150793jsn82c69f528d4e",
                "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
            }

            res = requests.get(URL, headers=headers)
            print(res.json())

            # write data into txt file 
            # save the txt file in data directory
            with open(f"{WORK_DIR}/airflow/data\{CURRENT_DATE}/{loc}.txt", "w") as write_file:
                write_file.write(json.dumps(res.json()))
                write_file.close()
    def get_or_create_bucket(self, bucket_name = f'weather_bucket_{CURRENT_DATE}'):

        # Aims to create bucket in google cloud if not exist
        try:
            bucket = STORAGE_CLIENT.create_bucket(bucket_or_name=bucket_name)
        except Conflict:
            bucket = STORAGE_CLIENT.get_bucket(bucket_or_name=bucket_name)
        
        return bucket
    
    def load_to_cloud(self, bucket_name = f'weather_bucket_{CURRENT_DATE}', overwrite = False):

        # get of create cloud storage bucket
        # Load today's weather txt file
        # overwrite to clean the past txt file content

        if overwrite: # delete bucket
            bucket = STORAGE_CLIENT.get_bucket(bucket_or_name=bucket_name)
            bucket.delete(force=True)
        
        # load data to bucket
        bucket = self.get_or_create_bucket(bucket_name=bucket_name)
        os.chdir(f"{WORK_DIR}/airflow/data/{CURRENT_DATE}")

        for file in os.listdir():
            blob = bucket.blob(file + f"_{CURRENT_DATE}_{datetime.datetime.now().strftime('%H:%M:%S')}")
            blob.upload_from_filename(file)

    def process_data(self):

        # load txt file content into pandas dataFrame

        os.chdir(f"{DATA_DIR}/{CURRENT_DATE}")
        files = os.listdir()

        data_df = pd.DataFrame()
        current_index = 0

        for file in files:
            with open(file,'r') as read_file:
                data = json.loads(read_file.read())

                # Extract data
                location_data = data.get("location")  
                current_data = data.get("current")

                # Create DataFrames
                location_df = pd.DataFrame(location_data, index=[current_index])
                current_df = pd.DataFrame(current_data, index=[current_index])
                current_index += 1
                current_df['condition'] = current_data.get('condition').get('text')

                # Concatenate DataFrames and append to main DataFrame
                temp_df = pd.concat([location_df, current_df],axis=1)
                data_df = pd.concat([data_df, temp_df])

                read_file.close()
            
        data_df = data_df.rename(columns={'name':'city'})
        data_df['localtime'] = pd.to_datetime(data_df['localtime'])
        return data_df
    
    def get_or_create_dataset(self,dataset_name = 'weather'):
        # get dataset or create it if not exist
        # return dataset in cloud (bigquery)

        print('Fetching Data.....\n')
        try:
            dataset = BIGQUERY_CLIENT.get_dataset(dataset_name)
            print('Done :)')
            print(dataset.self_link)
            return dataset
        except Exception as e:
            if e.code == 404:
                print('Dataset does not exist. Creating a new one.')
                BIGQUERY_CLIENT.create_dataset(dataset_name)
                dataset = BIGQUERY_CLIENT.get_dataset(dataset_name)
                print('Done')
                print(dataset.self_link)
                return dataset
            else:
                print(e)
    
    def get_or_ceate_table(self, dataset_name = 'weather', table_name = 'weather'):
        # create table in bigquery, if it exist return it
        # return Google BigQuery table

        dataset = self.get_or_create_dataset()
        project = dataset.project
        dataset_id = dataset.dataset_id
        table_id = f'{project}.{dataset_id}.{table_name}'

        print('\nFetching Table...\n')
        try:
            # get table
            table = BIGQUERY_CLIENT.get_table(table_id)
            print('Done. :)\n')
            print(table.self_link)
        except Exception as e:
            if e.code == 404:
                print('Table does not exist. Creating a new one......')
                BIGQUERY_CLIENT.create_table(table_id)
                table = BIGQUERY_CLIENT.get_table(table_id)
                print(table.self_link)
            else:
                print(e)
        finally:
            return table
    
    def load_data_to_bigquery(self, dataframe, dataset_name, table_name):
        # create dataset in bigquery,
        # if exist return it

        table = self.get_or_ceate_table(dataset_name=dataset_name, table_name=table_name)
        BIGQUERY_CLIENT.load_table_from_dataframe(dataframe=dataframe, destination= table)

    def train_model(self, dataset_name, table_name):
        # get table or create it if not exist
        # get dataset or create it if not exist
        # load data from BigQuery to DataFrame
        # Train model

        table = self.get_or_ceate_table(dataset_name=dataset_name, table_name=table_name)
        query = f'select * from {table}'
        df = BIGQUERY_CLIENT.query(query).to_dataframe()

        # Pre processing
        df = df.drop(columns = ['region', 'country', 'tz_id', 'localtime','last_updated_epoch', 'last_updated', 'wind_dir', 'condition'])
        city_map = {
                'London':0,
                'Moscow':1,
                'Berlin':2,
                'Paris':3,
                'Rome':4,
                'Madrid':5,
                'Cairo':6,
                'Tokyo':7,
                'Sydney':8}
        df['city'] = df['city'].map(city_map)

        # split to train test data
        X = df.drop(columns = ['temp_c'])
        y = df['temp_c']

        X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.9, random_state=365)

        # Model
        model = XGBRegressor()
        model.fit(X_train, y_train)

        # predict test data, generate accuracy score
        predictions = model.predict(X_test)
        score = model.score(X_test, y_test)

        # Generate and print a predictions dataframe
        cities = []
        for city_number in X_test.city.to_list():
            for city, num in city_map.items():
                if city_number == num:
                    cities.append(city)
        predictions_df = pd.DataFrame([*zip(cities, y_test, predictions, abs(y_test-predictions), [score]*len(cities))], columns=['city', 'actual_temp(Celcius)', 'predicted_temp(Celcius)', 'diff(Celcius)','score'])
        print(f"Test Data Predictions:\n {predictions_df}")
        
        return model
    
    def predict_next_day_weather(self, model, dataset_name, table_name):

        # use trained model to predict new data
        cities = ['London', 'Moscow' ,'Berlin' ,'Paris' ,'Rome' ,'Madrid', 'Cairo' ,'Tokyo', 'Sydney']
        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
        next_day = next_day.strftime("%Y-%m-%d")
        table = self.get_or_ceate_table(dataset_name=dataset_name, table_name=table_name)

        query = f"""WITH RankedWeather AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY city ORDER BY localtime DESC) AS rn
                    FROM
                        {table}
                    )
                    SELECT
                    *
                    FROM
                    RankedWeather
                    WHERE
                    rn = 1;"""
        new_data = BIGQUERY_CLIENT.query(query).to_dataframe()

        # preprocess
        new_data = new_data.drop(columns = ['temp_c','rn', 'region', 'country', 'tz_id', 'localtime','last_updated_epoch', 'last_updated', 'wind_dir', 'condition'])
        city_map = {
                'London':0,
                'Moscow':1,
                'Berlin':2,
                'Paris':3,
                'Rome':4,
                'Madrid':5,
                'Cairo':6,
                'Tokyo':7,
                'Sydney':8}
        new_data['city'] = new_data['city'].map(city_map)

        new_data['localtime_epoch'] = new_data['localtime_epoch'] + 86400

        predictions = model.predict(new_data)

        # Generate and print a predictions dataframe
        predictions_df = pd.DataFrame([*zip(cities, predictions)], columns=['city', 'predicted_temp(Celcius)'])
        predictions_df['at_date(UTC+0)'] = new_data['localtime_epoch']

        # translate epoch to datetime
        predictions_df['at_date(UTC+0)'] = pd.to_datetime(predictions_df['at_date(UTC+0)'], unit='s')
        print(f"Next Day Predictions:\n {predictions_df}")

        return predictions_df



if __name__ == "__main__":
    locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
    weather = WeatherPipeline(locations)
    weather.extract_data()
    weather.load_to_cloud()
    df = weather.process_data()
    weather.load_data_to_bigquery(dataframe = df, dataset_name='weather', table_name='weather')

    model = weather.train_model(dataset_name='weather', table_name='weather')
    predictions_df = weather.predict_next_day_weather(model, dataset_name='weather', table_name='weather')