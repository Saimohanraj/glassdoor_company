import pandas as pd
from datetime import datetime, timedelta
import os
import logging
import time
import boto3
import io
import tempfile
import json
import requests
# from dotenv import load_dotenv

# load_dotenv()


logger = logging.getLogger(__name__)
csv_dir = './'

# List to store the DataFrames
dfs = []

# Iterate over all CSV files in the directory
for file in os.listdir(csv_dir):
    if file.endswith('.csv'):
        file_path = os.path.join(csv_dir, file)
        df = pd.read_csv(file_path)
        dfs.append(df)

# Concatenate all DataFrames
combined_df = pd.concat(dfs, ignore_index=False)
combined_df.fillna('', inplace=True)
# combined_df = pd.read_csv('RhodeIsland_sample.csv',na_filter=True)
df_grouped = combined_df.groupby('company_url').agg({
    'id_categories': 'first',
    'company_url': 'first',
    'company_name': 'first',
    'company_size_category': 'first',
    'company_id': 'first',
    'company_headquarters': 'first',
    'location_url': 'first',
    'company_description': 'first',
    'company_review_url': 'first',
    'company_salary_url': 'first',
    'company_career_opportunities_rating': 'first',
    'company_compensation_and_benefits_rating': 'first',
    'company_culture_and_values_rating': 'first',
    'company_diversity_and_inclusion_rating': 'first',
    'company_overall_rating': 'first',
    'company_senior_management_rating': 'first',
    'company_work_life_balance_rating': 'first',
    'all_reviews_count': 'first',
    'salary_count': 'first',
    'hash': 'first',
    'industry': lambda x: list(set(x))
}).reset_index(drop=True)



current_day = datetime.now().weekday()
historical_new_data_dump = []
daily_data_dump = []
hash_dumps = []
session = boto3.Session()
s3_client = session.client('s3')
target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
destination_date = datetime.now().strftime("%Y-%m-%d")
spider_directory = 'glassdoor_company'


start_time = time.time()
output_buckt = 'lapis-lambda-outputs-mumbai'

spider_name ='glassdoor_company'
print(f'Spider Going to Start -----> {spider_name}')
hash_file_path = f'output/hash_collections/{spider_directory}/{target_date}/{spider_name}.txt'
hash_response = s3_client.get_object(Bucket=output_buckt, Key=hash_file_path)
file_content = hash_response['Body'].read().decode('utf-8')
hash_dump = file_content.split('\n')
hash_dumps = [element for element in hash_dump if element]




print('<---- Process Items ---->')
for index,row in df_grouped.iterrows():
    hash_name = row.get('hash')
    if hash_name in hash_dumps:
        print(f'Hash in the previous dump -----> {hash_name}')
    else:
        print(f'New Hash Detected ----> {hash_name}')
        historical_new_data_dump.append(dict(row))
        hash_dumps.append(hash_name)
    daily_data_dump.append(dict(row))

    # historical_new_data_dump.append(dict(row))
    # hash_dumps.append(hash_name)
    # daily_data_dump.append(dict(row))


json_upload_path = f'output/daily_collections/{spider_directory}/{destination_date}/{spider_name}.json'
parquet_upload_path = f'output/historical/{spider_directory}/{destination_date}/{spider_name}.parquet'
hash_file_path = f'output/hash_collections/{spider_directory}/{destination_date}/{spider_name}.txt'

# Prepare JSON dump
with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_file:
    temp_file.write("[\n")
    for index, data in enumerate(daily_data_dump):
        json.dump(data, temp_file, ensure_ascii=False)
        if index < len(daily_data_dump) - 1:
            temp_file.write(',\n')
    temp_file.write("\n]")
    temp_file_path = temp_file.name
    temp_file.flush()
    temp_file.seek(0)

# Upload JSON to S3
json_response = s3_client.put_object(Bucket=output_buckt, Key=json_upload_path, Body=open(temp_file_path, 'rb'))

# Prepare and upload Parquet file
new_df = pd.DataFrame(historical_new_data_dump, dtype=str)
final_df = new_df.drop('hash', axis=1)
with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
    tmp_paq_path = tmp_file.name
    final_df.to_parquet(tmp_paq_path, engine='fastparquet')
    tmp_file.flush()
    tmp_file.seek(0)
response = s3_client.put_object(Bucket=output_buckt, Key=parquet_upload_path, Body=open(tmp_paq_path, 'rb'))

# Write hash dumps to S3
hash_dump = [element + '\n' for element in hash_dumps]
with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as temp_file:
    temp_file.writelines(hash_dump)
    temp_file_path = temp_file.name
    temp_file.flush()
    temp_file.seek(0)
txt_response = s3_client.put_object(Bucket=output_buckt, Key=hash_file_path, Body=open(temp_file_path, 'rb'))
os.remove(temp_file_path)
s3_client.close()
print('completed')

    