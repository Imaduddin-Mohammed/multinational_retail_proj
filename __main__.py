from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from decouple import config


if __name__ == "__main__":

    # INITIALIZING THE THREE PROJECT CLASSES:
    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    # READING SENSITIVE INFORMATION AND INITIALIZING THE CONNECTOR METHOD:
    cred_path ='db_creds.yaml'
    credentials = connector.read_db_creds(file_path = cred_path)
    engine1, engine2 = connector.init_db_engine(credentials)

    # # TABLES IN THE AWS RDATABASE:
    # tables = connector.list_db_tables(engine1)
    # print("Tables in the database:")
    # for table in tables:
    #     print(table)

    # # EXTRACTING & CLEANING TABLES FROM AWS RDS:
    # legacy_users = extractor.read_rds_table(table_name= 'legacy_users', conn= engine1)
    # cleaned_legacy_users = cleaner.clean_legacy_user_data(legacy_users)
    # print(cleaned_legacy_users.info())

    # legacy_stores = extractor.read_rds_table(table_name= 'legacy_store_details', conn= engine1)
    # cleaned_legacy_store = cleaner.clean_legacy_store_data(legacy_stores)
    # print(cleaned_legacy_store.info())

    # orders_df = extractor.read_rds_table(table_name= 'orders_table', conn= engine1)
    # cleaned_orders_df = cleaner.clean_orders_data(orders_df)
    # print(cleaned_orders_df.info())

    # # UPLOADING CLEANED LEGACY USERS TABLE TO SALES DATABSE:
    # users = connector.upload_to_db(cleaned_legacy_users, 'dim_users', engine2)
    # print("Successfully uploaded legacy users table as 'dim_users' to sales database.")

    # # EXTRACTING CARD DETAILS FROM PDF:
    # pdf_path = config('PDF_PATH')
    # extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    # extracted_card_df

    # # CLEANING THE CARD DETAILS:
    # cleaned_card_data = cleaner.clean_card_data(card_data= extracted_card_df)
    # print(cleaned_card_data.info())

    # # UPLOADING THE CARD DETAILS TO SALES DATBASE:
    # card = connector.upload_to_db(cleaned_card_data, 'dim_card_details', engine2)
    # print("Successfully uploaded card details as 'dim_card_details' to sales database")

    # # EXTRACTING STORES DATA USING AN API:

    # # DEFINING SENSITIVE INFO (we have 2-api endpoints)
    # number_of_stores_ep = config('NO_OF_STORES_EP')
    # retrieve_a_store_ep= config('RETRIEVE_A_STORE_EP')
    # headers = config('HEADER_DETAILS')

    # number_of_stores = extractor.list_number_of_stores(number_of_stores_ep, headers)
    # print(f"There are {number_of_stores} stores.")
    # stores_df = extractor.retrieve_stores_data(number_of_stores_ep, retrieve_a_store_ep, headers)
    # print(stores_df.info())

    # # CLEANING STORES DATA:
    # cleaned_stores_df = cleaner.clean_store_data(stores_df)
    # print(cleaned_stores_df.info())

    # # UPLOADING STORES DATA TO SALES DATABASE:
    # stores_df = connector.upload_to_db(cleaned_stores_df, 'dim_store_details', engine2)
    # print("Successfully uploaded stores data as 'dim_store_details' to sales database")

    # EXTRACTING PRODUCTS DATA FROM AWS S3 BUCKET:     ****very important!- make sure to configure aws credentials before running this method -very important!****
    # defining the credentials that will be required for this task
    # s3_address = config('S3_ADDRESS')
    # csv_filepath = config('CSV_FILEPATH')

    # product_df, converted_products_csv = extractor.extract_from_s3(s3_address, csv_filepath)
    # print(product_df.info())

    # # CLEANING PRODUCTS DATA:
    # filtered_products_df = cleaner.clean_products_data(product_df)
    # print(filtered_products_df)
    # # we convert the weight column which consists of values of multiple units of measurement to one unit of measurement i.e 'kg'.
    # cleaned_products_df = cleaner.convert_product_weights(filtered_products_df)
    # print(cleaned_products_df.info())

    # # UPLOADING PRODUCTS DATA TO SALES DATABASE:
    # dim_products = connector.upload_to_db(cleaned_products_df, "dim_products", engine2)
    # print("Successfully uploaded products data as 'dim_product_details' to sales database.")

    # # UPLOADING THE ORDERS TABLE WHICH WAS EXTRACTED AND CLEANED AT THE START FROM AWS RDS:
    # orders = connector.upload_to_db(cleaned_orders_df, 'dim_orders', engine2)
    # print("Successfully uploaded orders data as 'dim_orders' in sales database")


    








    

    






    