# Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.
# It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
# Use your list_db_tables method to get the name of the table containing user data.
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.

import pandas as pd
from database_utils import DatabaseConnector

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

if __name__ =="__main__":
    connector = DatabaseConnector()
    conn = connector.init_db_engine()
    table_name = 'legacy_users'
    extractor = DataExtractor()
    extracted_table = extractor.read_rds_table(table_name, conn)
    print(extracted_table)
