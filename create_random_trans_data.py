# Create a random transactional data set for pizza orders
import json
from faker import Faker
import random
import pandas as pd
import boto3

fake = Faker('en_US')
pizza_types = ['cheese', 'pepperoni', 'supreme', 'meat lover', 'veggie']

df = pd.DataFrame()
for _ in range(100):
	my_dict = { 'order_id': random.randint(0,1000), 'customer_id': random.randint(0,10),
				'type': random.choice(pizza_types), 'qty': random.randint(1,5),
				'retail_price': float(random.randrange(2000, 5500)/100),
				'order_date': fake.date_time_between(start_date='-2y', end_date='now') }
	df = df.append(my_dict, ignore_index=True)

print(df)

file_name = 'pizza_orders.csv'
df.to_csv(file_name,  index=False)

bucket_name = 'torqata'
folder_name = 'pizza/'
s3 = boto3.resource('s3')
s3.meta.client.upload_file(file_name, bucket_name, folder_name+file_name)
