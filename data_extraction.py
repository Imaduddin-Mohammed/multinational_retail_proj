# Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.
# It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
# Use your list_db_tables method to get the name of the table containing user data.
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.

import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        self.connector = DatabaseConnector()
    def read_rds_table(self, table_name):
        try:
            self.connector.init_db_engine()
            with self.connector.connect() as connection:
                query = f"SELECT * FROM {table_name}"
                result = connection.execute(query)
                df = pd.DataFrame(result.fetchall(), columns = result.keys())
                return df
        except Exception as e:
            print(f"Error extracting data from {table_name}: {e}")
            return None

#Reading table names using the connector instance of DatabaseConnector class:
if __name__ == "__main__":
    connector = DatabaseConnector()
    connector.init_db_engine()
    table_names = connector.list_db_tables()
    for table in table_names:
        print(table)
    
    table_name = 'legacy_users'
    extractor = DataExtractor()
    extracted_table = extractor.read_rds_table(table_name)
    print(extracted_table)
