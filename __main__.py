from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from decouple import config


if __name__ == "__main__":

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    cred_path ='db_creds.yaml'
    credentials = connector.read_db_creds(file_path = cred_path)

    # Estabilishing connection to the database
    # engine1, engine2 = connector.init_db_engine(credentials)
    # tables = connector.list_db_tables(engine1)
    # print("Tables in the database:")
    # for table in tables:
    #     print(table)

    # Data Extraction:
    # extracted_table = extractor.read_rds_table(table_name, conn = engine1)
    # print(extracted_table)

    # pdf_path = credentials['pdf_path']
    # extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    # print(extracted_card_df.info())

    # headers = credentials['header_details']
    # return_the_number_of_stores = credentials['return_the_number_of_stores']
    # number_of_stores = extractor.list_number_of_stores(return_the_number_of_store, headers)
    # print(f"There are {number_of_stores} stores.")

    # retrieve_a_store = credentials['retrieve_a_store']
    ## headers same as previous
    ## passing in number_of_stores as an argument from the above method
    # stores_df = extractor.retrieve_stores_data(number_of_stores, retrieve_a_store, headers)
    # print(stores_df.info())

    s3_address = config('S3_ADDRESS')
    csv_filepath = config('CSV_FILEPATH')
    #make sure to configure aws credentials before running this method
    extracted_product_df, csv_file = extractor.extract_from_s3(s3_address, csv_filepath)
    print(extracted_product_df.info())

    



    

    






    