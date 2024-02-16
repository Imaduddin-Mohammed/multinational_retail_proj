# Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.
# It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
# Use your list_db_tables method to get the name of the table containing user data.
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.

import pandas as pd
from database_utils import DatabaseConnector
import tabula

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


if __name__ =="__main__":
    
    connector = DatabaseConnector()
    engine1, engine2 = connector.init_db_engine()

    #passing the table names obtained from database_utils class and printing the extractor class result
    table_name = 'legacy_users'
    extractor = DataExtractor()
    extracted_table = extractor.read_rds_table(table_name, conn = engine1)
    print(extracted_table)

    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    print(extracted_card_df)
    