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

    def clean_user_data(self, legacy_user):

        pd.set_option('display.max_columns', None)
        #created copy of the dataframe
        user_df = legacy_user.copy()
        #checking for null values in the dataframe
        user_df.dropna(inplace = True)
        #removing any duplicate values from the dataframe
        user_df.drop_duplicates( inplace = True)
        user_df.first_name = user_df.first_name.astype('string')
        user_df.last_name = user_df.last_name.astype('string')
        user_df.join_date = pd.to_datetime(user_df['join_date'],infer_datetime_format= True, errors= 'coerce')
        user_df.date_of_birth = pd.to_datetime(user_df['date_of_birth'], infer_datetime_format= True, errors= 'coerce')
        user_df.country = user_df.country.astype('string')
        #removed nulls from countries and kept the mask of only these 3
        countries_to_keep = ['United Kingdom', 'Germany', 'United States']
        user_df.country = user_df.country[user_df['country'].isin(countries_to_keep)]
        return user_df
    

    def clean_store_data(self, store_data):

        pd.set_option('display.max_columns', None)
        store_df = store_data.copy()
        store_df.drop(columns = ['lat'], inplace = True)
        col_with_null_percent = store_df.isna().mean()*100
        print(col_with_null_percent)
        #row 447 in the entire store_df contains incorrect value
        store_df.drop([447], inplace = True)
        #converting the staff_numbers column to numeric
        store_df.staff_numbers = pd.to_numeric(store_df.staff_numbers, errors='coerce')
        print(store_df.staff_numbers)
        #keeping only 2 continents
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        store_df.continent = store_df.continent[store_df.continent.isin(continents_to_keep)]
        #using .replace to map correctly
        store_df['continent'] = store_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        store_type_to_keep = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        store_df.store_type = store_df.store_type[store_df.store_type.isin(store_type_to_keep)]
        print(store_df.store_type.value_counts())
        store_df.opening_date = pd.to_datetime(store_df.opening_date, infer_datetime_format= True, errors= 'coerce')
        print(store_df.opening_date)
        store_df.reset_index()
        print(store_df.continent.value_counts())
        return store_df



    def clean_orders_data(self, orders_data):

        pd.set_option('display.max_columns', None)
        order_df = orders_data.copy()
        #dropping these 2 columns because they are irrelevant
        order_df.drop(columns = ['level_0', '1'], inplace = True)
        #checking productquantity for incorrect values
        print(order_df.product_quantity.value_counts())
        null_cols_percent = print(order_df.isna().mean()*100)
        print(null_cols_percent)
        order_df.card_number.drop_duplicates(inplace = True)
        order_df.dropna(subset = ['first_name', 'last_name'], inplace = True)
        return order_df

if __name__ =="__main__":
    connector = DatabaseConnector()
    conn = connector.init_db_engine()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    # legacy_users_table = extractor.read_rds_table(table_name= 'legacy_users', conn = conn)
    # cleaned_legacy_user = cleaner.clean_user_data(legacy_users_table)
    # print(cleaned_legacy_user)

    legacy_store_table = extractor.read_rds_table(table_name= 'legacy_store_details', conn = conn)
    cleaned_legacy_store = cleaner.clean_store_data(legacy_store_table)
    print(cleaned_legacy_store)


    # orders_table = extractor.read_rds_table(table_name= 'orders_table', conn = conn)
    # cleaned_orders_table = cleaner.clean_orders_data(orders_table)
    # print(cleaned_orders_table)

        
        
    