import psycopg2
import boto3
import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_file_from_s3(bucket_name, folder_name, file_name):
	s3 = boto3.resource('s3')
	s3.Bucket(bucket_name).download_file(folder_name + file_name, file_name)
	df = pd.read_csv(file_name)
	return df


# connect to postgress and get customer data
def connect_to_postgres():
	postgres_host = os.getenv('POSTGRES_HOST')
	postgres_user = os.getenv('POSTGRES_USER')
	postgres_pass = os.getenv('POSTGRES_PASS')

	conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_pass)
	cur = conn.cursor()
	return cur

cur = connect_to_postgres()
cur.execute('SELECT * from customers')
customer_data = cur.fetchall()
desc = cur.description

cols = []
for i in range(0, len(desc)):
	cols.append(desc[i][0])

customers = pd.DataFrame(customer_data, columns=cols)

bucket_name = 'torqata'
# get transactional data
pizza_trans = get_file_from_s3(bucket_name, 'pizza/', 'pizza_orders.csv')

# get state population data
state_pop = get_file_from_s3(bucket_name, 'us_state_pop/', 'state_population_2019.csv')

# merge data sets
pizza_orders = pd.merge(pizza_trans, customers, left_on='customer_id',
						right_on='customer_id', how='left')

pizza_orders = pd.merge(pizza_orders, state_pop, left_on='state',
						right_on='NAME', how='left')
pizza_orders['order_date'] = pd.to_datetime(pizza_orders['order_date'])

# get orders for the last 12 months
twelve_months = datetime.now() - relativedelta(months=+12)
df12 = pizza_orders.loc[pizza_orders['order_date']>=twelve_months]

# number of pizzas per type sold in the past 12 months
pizza_qty = df12[['type', 'qty']]
pizza_qty = pizza_qty.groupby('type').sum().reset_index()

# tot. sales of pizzas per type sold in the past 12 months
tot_sales = df12[['type', 'retail_price']]
tot_sales = tot_sales.groupby('type').sum().reset_index()

# tot. sales of pizzas per capita per type in the past 12 months
per_state = df12[['type', 'state', 'qty']]
per_state = per_state.groupby(['type', 'state']).sum().reset_index()
per_state = per_state.pivot(index='type', columns='state', values='qty').reset_index()


# number of unique customers per type in the past 12 months
customers = df12[['customer_id', 'type']]
customers = customers.groupby('type').agg({'customer_id': 'nunique'}).reset_index()
customers = customers.rename(columns={'customer_id': 'no_unique_customers'})

# merge calculation dfs
denom_df = pd.merge(pizza_qty, tot_sales, left_on='type', right_on='type', how='left')
denom_df = pd.merge(denom_df, customers, left_on='type', right_on='type', how='left')
denom_df = pd.merge(denom_df, per_state, left_on='type', right_on='type', how='left')
denom_df = denom_df.fillna(0)

# add in state columns if not in transactional data
list_of_columns = ['type', 'qty', 'retail_proce', 'no_unique_customers', 'alabama',
					'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
					'delaware', 'district_of_columbia', 'florida', 'georgia', 'hawaii',
					'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
					'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
					'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new_hampshire',
					'new_jersey', 'new_mexico', 'new_york', 'north_carolina', 'north_dakota',
					'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'puerto_rico', 'rhode_island',
					'south_carolina', 'south_dakota', 'tennessee', 'texas', 'utah', 'vermont',
					'virginia', 'washington', 'west_virginia', 'wisconsin', 'wyoming']
for col_name in list_of_columns:
	if col_name not in denom_df.columns:
		denom_df[col_name] = 0


# save df to s3
file_name = 'summarized_transactions.csv'
bucket_name = 'torqata'
folder_name = 'summarized/'
denom_df.to_csv(file_name, index=False)
s3 = boto3.resource('s3')
s3.meta.client.upload_file(file_name, bucket_name, folder_name+file_name)

