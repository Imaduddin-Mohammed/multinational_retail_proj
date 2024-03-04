# Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.
# It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
# Use your list_db_tables method to get the name of the table containing user data.
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.

import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from decouple import config



class DataExtractor:

    
    def read_rds_table(self, table_name, conn):
        try:
            result = pd.read_sql_table(table_name, conn)
            print(type(result))
            return result
        except Exception as e:
            print(f"Error extracting data from {table_name}: {e}")
            return None
        
    def retrieve_pdf_data(self, pdf_path):
        # multiple_tables=True is used to ensure that each page of the PDF is treated as a separate table. Then, pd.concat() is used to concatenate all the tables into a single DataFrame. Finally, reset_index() is used to reset the index of the concatenated DataFrame to ensure it's consistent.
        pdf_data = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
        # Concatenate all tables into a single DataFrame
        concatenated_card_data = pd.concat(pdf_data)
        # Reset the index of the concatenated DataFrame
        concatenated_card_data.reset_index(drop=True, inplace=True)
        return concatenated_card_data
    
    def list_number_of_stores(self, return_the_number_of_stores, headers):
        try:
            response = requests.get(return_the_number_of_stores, headers = headers)    
            if response.status_code == 200:
                stores = response.json()
                return stores['number_stores']
            else:
                print("Failed to fetch number of stores. Status code:", response.status_code)
                return None
        except requests.RequestException as e:
            print(f"An error occured: {e}")
            return None
            
    def retrieve_stores_data(self, number_of_stores, retrieve_a_store, headers):
        try:
            store_list = []
            for store_number in range(0, number_of_stores):
                #we can also use .format method to replace the store number
                endpoint_url = retrieve_a_store.replace("{store_number}", str(store_number))
                response = requests.get(endpoint_url, headers = headers)

                if response.status_code ==200:
                    store_data = response.json()
                    store_list.append(store_data)

                else:
                    f'Error retrieving store_details:{response.text}'
                    return f'error code:{response.status_code}'

            store_df = pd.DataFrame(store_list)
            print("Retrieving stores dataframe:")
            return store_df
        
        except requests.RequestException as e:
            print(f"An error occured: {e}")
            return None


    def extract_from_s3(self, s3_address, csv_filepath ):
        #make sure to configure aws credentials before running this method
        try:
            s3 = boto3.client('s3')
            #we were not given the bucket name and object key hence we scrape it from the address by splitting it
            bucket_name, object_key = s3_address.replace('s3://', '').split('/', 1)
            s3.download_file(bucket_name, object_key, csv_filepath)
            products_df = pd.read_csv(csv_filepath, index_col=0)  # Read the .csv file into a Pandas DataFrame
            print(f"Extracted successfully to this path: {csv_filepath} ")
            return products_df, products_df.to_csv(index = False)
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)






    