import sqlalchemy
import yaml


class DatabaseConnector:
    def __init__(self, file_path='db_creds.yaml'):
        self.file_path = file_path
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        try:
            with open(self.file_path, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{self.file_path}' not found. Make sure it exists.")

    def init_db_engine(self):
        db_url1 = f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        db_url2 = f"postgresql://{self.db_creds['USER']}:{self.db_creds['PASSWORD']}@{self.db_creds['HOST']}:{self.db_creds['PORT']}/{self.db_creds['DATABASE']}"
        self.engine1 = sqlalchemy.create_engine(db_url1)
        self.engine2 = sqlalchemy.create_engine(db_url2)
        return self.engine1, self.engine2
        

    def list_db_tables(self):
        with self.engine1.connect() as connection:
            inspector = sqlalchemy.inspect(connection)
            table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self,dataframe,table_name):
        dim_users = dataframe.to_sql(table_name,self.engine2, if_exists = 'replace')
        return dim_users


if __name__ == "__main__":
    connector = DatabaseConnector()
    connector.init_db_engine()
    tables = connector.list_db_tables()
    print("Tables in the database:")
    for table in tables:
        print(table)


        