from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":
    connector = DatabaseConnector()
    engine1, engine2 = connector.init_db_engine()
    extractor = DataExtractor()
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    extracted_card_df = extractor.retrieve_pdf_data(pdf_path)
    print(extracted_card_df)
    cleaner = DataCleaning()
    cleaned_card_df = cleaner.clean_card_data(extracted_card_df)
    print(cleaned_card_df)




    