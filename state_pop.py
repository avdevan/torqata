# Retrieve US state population data
import requests
import pandas as pd
from pandas import DataFrame
import os
import boto3

census_key = os.getenv('CENSUS_KEY')
year = '2019'
dsource = 'pep'
dname = 'population'
cols = 'NAME,POP'

# get US census data for the population of each state
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=state:*&key={census_key}'
response=requests.get(data_url).json()

df = DataFrame(response[1:], columns=response[0]).drop(columns=['state'])

file_name = 'state_population_2019.csv'
df.to_csv(file_name,  index=False)

bucket_name = 'torqata'
folder_name = 'us_state_pop/'
s3 = boto3.resource('s3')
s3.meta.client.upload_file(file_name, bucket_name, folder_name+file_name)
