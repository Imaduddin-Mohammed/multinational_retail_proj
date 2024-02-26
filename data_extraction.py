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

class DataExtractor:
    def __init__(self):
        pass

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
    
    def list_number_of_stores(self, no_of_stores_endpoint, headers):
        # Make a request to the API key URL
        response = requests.get(no_of_stores_endpoint, headers=headers)    
        # Check if the request was successful
        if response.status_code == 200:
            # Extract the number of stores from the response
            stores = response.json()
            return f"The number of stores to extract :\n{stores}"
        else:
            # Handle unsuccessful request
            print("Failed to fetch number of stores. Status code:", response.status_code)
            return None

    def retrieve_stores_data(self):
        for number in range(int(stores['number_stores'])):
            url_base = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{number}"
            response = requests.get(url_base)
            if response.status_code == 200:
                store = response.json()
                return type(store)
            else:
                # Handle unsuccessful request
                print("Failed to retrieve stores data. Status code:", response.status_code)
                return response.text
        

    # def extract_from_s3(self, address):
    #     try:
    # # Boto3 code that may raise exceptions
    #         s3 = boto3.client('s3')
    #         file = s3.download_file('my-boto3-bucket-imad', 'pio.jpeg', "\\Users\\mohdi\\Desktop\\pio_from_s3_bucket.jpeg")
    #         print(file)
    #         # response = s3.download_file('data-handling-public', 'products.csv', '\\Users\\mohdi\\Desktop\\aicore\\products.csv')
    #     except NoCredentialsError:
    #         print("AWS credentials not found. Please configure your credentials.")

    #     except ClientError as e:
    #         if e.response['Error']['Code'] == 'NoSuchBucket':
    #             print("The specified bucket does not exist.")
    #         else:
    #             print("An error occurred:", e)
            
if __name__ =="__main__":
    
    connector = DatabaseConnector()
    engine1, engine2 = connector.init_db_engine()

    #passing the table names obtained from database_utils class and printing the extractor class result
    table_name = 'legacy_users'
    extractor = DataExtractor()

    # extracted_table = extractor.read_rds_table(table_name, conn = engine1)
    # print(extracted_table)

    # pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    # print(extracted_card_df)

    no_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    stores = extractor.list_number_of_stores(no_of_stores_endpoint, headers)
    print(stores)

    store_df = extractor.retrieve_stores_data()
    print(store_df)

    # address = "s3://data-handling-public/products.csv"
    # extracted_product_data = extractor.extract_from_s3()
    # print(extracted_product_data)

    



    