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
        db_url = f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        self.engine = sqlalchemy.create_engine(db_url)

    def list_db_tables(self):
        with self.engine.connect() as connection:
            inspector = sqlalchemy.inspect(connection)
            table_names = inspector.get_table_names()
        return table_names

# Example usage:
if __name__ == "__main__":
    connector = DatabaseConnector()
    connector.init_db_engine()
    tables = connector.list_db_tables()
    print("Tables in the database:")
    for table in tables:
        print(table)

        


        