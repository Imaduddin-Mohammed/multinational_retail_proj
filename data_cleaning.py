# Step 6:
# Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
# You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
# Step 7:
# Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
# Step 8:
# Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, df):
        pd.set_option('display.max_columns', None)
        cleaned_data = df.copy()
        print(cleaned_data.head())
        print(cleaned_data.info())
        print(cleaned_data.isna().mean()*100)
        print(cleaned_data.value_counts())
        cleaned_data.first_name = cleaned_data.first_name.astype('string')
        cleaned_data.last_name = cleaned_data.last_name.astype('string')
        cleaned_data.address = cleaned_data.address.astype('string')
        cleaned_data.country = cleaned_data.country.astype('string')
        cleaned_data.country_code = cleaned_data.country_code.astype('string')
        cleaned_data.company = cleaned_data.company.astype('string')
        cleaned_data.email_address = cleaned_data.email_address.astype('string')
        cleaned_data.join_date = pd.to_datetime(cleaned_data.join_date, errors = 'coerce')
        cleaned_data.date_of_birth = pd.to_datetime(cleaned_data.date_of_birth, errors = 'coerce')
        cleaned_data.phone_number = pd.to_numeric(cleaned_data.phone_number, errors='coerce')
        cleaned_data.user_uuid = pd.to_numeric(cleaned_data.user_uuid, errors='coerce')
        print(cleaned_data.join_date.info())
        print(cleaned_data.date_of_birth.info())
        # cleaned_data = cleaned_data.dropna(subset=['Column3','Column4'])
        return cleaned_data.info()

if __name__ =="__main__":
    connector = DatabaseConnector()
    conn = connector.init_db_engine()
    extractor = DataExtractor()
    df = extractor.read_rds_table(table_name= 'legacy_users', conn = conn)
    cleaner = DataCleaning()
    cleaned_df = cleaner.clean_user_data(df)
    print(cleaned_df)
        
        
    