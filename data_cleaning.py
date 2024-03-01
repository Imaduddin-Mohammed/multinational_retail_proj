# Step 6:
# Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
# You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
# Step 7:
# Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
# Step 8:
# Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from dateutil.parser import parse
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
        store_df.staff_numbers = pd.to_numeric(store_df.staff_numbers, errors= 'ignore')
        #keeping only 2 continents
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        store_df.continent = store_df.continent[store_df.continent.isin(continents_to_keep)]
        #using .replace to map correctly
        store_df['continent'] = store_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        store_type_to_keep = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        store_df.store_type = store_df.store_type[store_df.store_type.isin(store_type_to_keep)]
        store_df.opening_date = pd.to_datetime(store_df.opening_date, infer_datetime_format= True, errors= 'coerce')
        store_df.reset_index()
        print(store_df.head(5))
        return store_df

    def clean_orders_data(self, orders_data):

        pd.set_option('display.max_columns', None)
        order_df = orders_data.copy()
        #dropping these 2 columns because they are irrelevant
        order_df.drop(columns = ['level_0', '1'], inplace = True)
        #checking productquantity for incorrect values
        null_cols_percent = print(order_df.isna().mean()*100)
        print(null_cols_percent)
        order_df.card_number.drop_duplicates(inplace = True)
        order_df.dropna(subset = ['first_name', 'last_name'], inplace = True)
        return order_df
    
    #Task 4 step 3
    #DataCleaning class to clean the data to remove any erroneous values, NULL values or errors with formatting.
    def clean_card_data(self, card_data):
        # pd.set_option('display.max_rows', None)
        extracted_card_df = card_data.copy()
        print(extracted_card_df.date_payment_confirmed.head())
        print(f"Value count is: \n{extracted_card_df.date_payment_confirmed.value_counts()}")
        #we mask out these NULLS for the entire dataframe
        Nulls = ['NULL']
        mask = extracted_card_df.date_payment_confirmed.isin(Nulls)
        subset_df = extracted_card_df[mask]
        subset_df
        #from above we can see that there are 11NULL values present in this column which looking upon closely are entirely NULL for each column in the entire dataframe
        extracted_card_df.dropna(subset=['date_payment_confirmed'], inplace=True)
        #This doesnt drop nulls because they are not recognised by pandas because they are string
        print(extracted_card_df.date_payment_confirmed.iloc[377])
        #we check if NULLS are dropped. But they arent
        #we have to manually handle these NULL values in the dataframe because they might be string or in a format pandas cannot recognise.
        extracted_card_df.replace('NULL', float("NaN"), inplace= True)
        extracted_card_df.replace('', float("NaN"), inplace= True)
        extracted_card_df.replace(' ', float("NaN"), inplace= True)
        print(extracted_card_df.date_payment_confirmed.iloc[377])
        #dropping rows with all NaN values
        extracted_card_df.dropna(axis = 0, how='all', inplace= True)
        extracted_card_df.card_provider.value_counts()
        #Removing the gibberish values from the dataframe and saving it in a filtered dataframe namely df.
        gibberish_values = ['OGJTXI6X1H', 'BU9U947ZGV', 'UA07L7EILH', 'XGZBYBYGUW', 'DLWF2HANZF', '1M38DYQTZV', 'JRPRLPIBZ2',  'DE488ORDXY', '5CJH7ABGDR', 'JCQMU8FN85', 'TS8A81WFXV', 'WJVMUO4QX6', 'NB71VBAHJE', '5MFWFBZRM9']
        df = extracted_card_df[~extracted_card_df.card_provider.isin(gibberish_values)]
        date_format = '%m/%y'
        df.expiry_date = pd.to_datetime(df.expiry_date, format = date_format, errors= 'coerce')
        df.expiry_date
        #applying parse because there are inconsistent date strings in this column, due to which format fails.
        df.date_payment_confirmed = df.date_payment_confirmed.apply(parse)
        return df
    

    #Task 2 Step 4
    def clean_store_data(self, store_df):
        store_data = store_df.copy()
        store_df.head(12)
        store_data.isna().mean()*100
        store_data.continent.value_counts()
        store_data.replace('NULL', float("NaN"), inplace= True)
        store_data.isna().mean()*100
        store_data.value_counts()
        #removing gibberish values from the dataframe
        gibberish_values = ['QMAVR5H3LD', 'LU3E036ZD9', '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        cleaned_store_data = store_data[~store_data.continent.isin(gibberish_values)]
        cleaned_store_data.drop('lat', axis = 1 , inplace= True)
        cleaned_store_data.continent.replace('eeEurope', 'Europe', inplace= True)
        cleaned_store_data.continent.replace('eeAmerica', 'America', inplace= True)
        cleaned_store_data.dropna(how = 'all', axis= 0, inplace= True)
        cleaned_store_data.opening_date = cleaned_store_data.opening_date.astype('datetime64[ns]')
        cleaned_store_data.continent = cleaned_store_data.continent.astype('category')
        cleaned_store_data.country_code = cleaned_store_data.country_code.astype('category')
        return cleaned_store_data #3 rows contain entire NULL values, 217,405 & 437.

        

if __name__ =="__main__":
    connector = DatabaseConnector()
    engine1, engine2 = connector.init_db_engine()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    # legacy_users_table = extractor.read_rds_table(table_name= 'legacy_users', conn = engine1)
    # cleaned_legacy_user = cleaner.clean_user_data(legacy_users_table)
    # print(cleaned_legacy_user)

    # legacy_store_table = extractor.read_rds_table(table_name= 'legacy_store_details', conn = engine1)
    # cleaned_legacy_store = cleaner.clean_store_data(legacy_store_table)
    # print(cleaned_legacy_store)

    # orders_table = extractor.read_rds_table(table_name= 'orders_table', conn = engine1)
    # cleaned_orders_table = cleaner.clean_orders_data(orders_table)
    # print(cleaned_orders_table)

    ## uploading legacy_users table to sales_data database in a table called dim_users
    # users = connector.upload_to_db(cleaned_legacy_user, 'dim_users')
    # print(type(users))
    
    ## extracting card_details from pdf
    # pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    # extracted_card_df
    # cleaned_card_data = cleaner.clean_card_data(card_data= extracted_card_df)
    # print(cleaned_card_data.info())

    ##uploading cleaned_card_data to sales_database in a table called dim_card_details
    # card = connector.upload_to_db(cleaned_card_data, 'dim_card_details')
    # print("successfully uploaded card details to the sales_data database")
    # print(type(card))

    ##extracting store details from the endpoint url:
    # return_the_number_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # retrieve_a_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    # headers = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    # number_of_stores = extractor.list_number_of_stores(return_the_number_of_stores)
    # store_df = extractor.retrieve_stores_data(number_of_stores, retrieve_a_store)
    # cleaned_store_data = cleaner.clean_store_data(store_df)
    # print(cleaned_store_data.info())

    ## uploading cleaned_store_data to sales_databse in a table called dim_store_details
    # store = connector.upload_to_db(cleaned_store_data, 'dim_store_details')
    # print("Successfully uploaded cleaned store data to the sales_database")

    








