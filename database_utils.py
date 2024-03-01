import sqlalchemy
import yaml


class DatabaseConnector:

    def read_db_creds(self, file_path):
        try:
            with open(file_path, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{file_path}' not found. Make sure it exists.")

    def init_db_engine(self, credentials):
        db_url1 = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        db_url2 = f"postgresql://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}"
        engine1 = sqlalchemy.create_engine(db_url1)
        engine2 = sqlalchemy.create_engine(db_url2)
        return engine1, engine2
        

    def list_db_tables(self, engine1):
        with engine1.connect() as connection:
            inspector = sqlalchemy.inspect(connection)
            table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self,dataframe,table_name, engine2):
        dim_users = dataframe.to_sql(table_name,engine2, if_exists = 'replace')
        return dim_users


if __name__ == "__main__":

    connector = DatabaseConnector()
    connector.init_db_engine()
    tables = connector.list_db_tables()
    print("Tables in the database:")
    for table in tables:
        print(table)


        