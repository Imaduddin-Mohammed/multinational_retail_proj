from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    cred_path ='db_creds.yaml'
    credentials = connector.read_db_creds(file_path = cred_path)
    # engine1, engine2 = connector.init_db_engine(credentials)
    # tables = connector.list_db_tables(engine1)
    # print("Tables in the database:")
    # for table in tables:
    #     print(table)

    



    

    






    