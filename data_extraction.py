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

    
    def read_rds_table(self, table_name, conn):
        try:
            result = pd.read_sql_table(table_name, conn)
            print(type(result))
            return result
        except Exception as e:
            print(f"Error extracting data from {table_name}: {e}")
            return None
    def retrieve_pdf_data(self, credentials):
        # multiple_tables=True is used to ensure that each page of the PDF is treated as a separate table. Then, pd.concat() is used to concatenate all the tables into a single DataFrame. Finally, reset_index() is used to reset the index of the concatenated DataFrame to ensure it's consistent.
        pdf_data = tabula.read_pdf(credentials['pdf_path'], pages='all', multiple_tables=True)
        # Concatenate all tables into a single DataFrame
        concatenated_card_data = pd.concat(pdf_data)
        # Reset the index of the concatenated DataFrame
        concatenated_card_data.reset_index(drop=True, inplace=True)
        return concatenated_card_data
    
    def list_number_of_stores(self, credentials):
        try:
            response = requests.get(credentials['return_the_number_of_stores'], headers = credentials['header_details'])    
            if response.status_code == 200:
                stores = response.json()
                return stores['number_stores']
            else:
                print("Failed to fetch number of stores. Status code:", response.status_code)
                return None
        except requests.RequestException as e:
            print(f"An error occured: {e}")
            return None
            
    def retrieve_stores_data(self, number_of_stores, credentials):
        try:
            store_list = []
            for store_number in range(0, number_of_stores):
                endpoint_url = credentials['retrieve_a_store'].replace("{store_number}", str(store_number))
                response = requests.get(endpoint_url, headers = credentials['header_details'])

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


    def extract_from_s3(self, credentials):
        s3 = boto3.client('s3')

        file = s3.download_file()
        print(file)



            
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

    # return_the_number_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # retrieve_a_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    # headers = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    # number_of_stores = extractor.list_number_of_stores(return_the_number_of_stores)
    # print(f"There are {number_of_stores} stores.")

    # stores_df = extractor.retrieve_stores_data(number_of_stores, retrieve_a_store)
    # print(stores_df.info())

    # address = "s3://data-handling-public/products.csv"
    # extracted_product_data = extractor.extract_from_s3()
    # print(extracted_product_data)

    



    