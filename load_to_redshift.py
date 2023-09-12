import pandas as pd
from sqlalchemy import create_engine, exc, types
import os

class RedshiftDataLoader:
    def __init__(self, db_user, db_pass, db_host):
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.engine = self.connect_to_database()

    def connect_to_database(self):
        connection_url = f'redshift+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:5439/data-engineer-database'
        engine = create_engine(connection_url, pool_pre_ping=True)
        try:
            # Attempt to connect to the database
            connection = engine.connect()
            print("Database connection successful")
            connection.close()
        except exc.SQLAlchemyError as e:
            print('Connection failed with error', e)
        return engine

    def load_data_to_database(self, openaq_df, table_name='open_aq_data'):
        try:
            openaq_df.to_sql(table_name, 
                             self.engine, 
                             if_exists='replace', 
                             index=False, 
                             dtype={'parameters': types.VARCHAR(length=65535)})
            print('Data successfully loaded')
        except exc.SQLAlchemyError as e:
            print('SQLAlchemy Exception: ', e)
        

def main(openaq_df):
    db_user = os.environ.get('redshift_db_user')
    db_pass = os.environ.get('redshift_db_pass')
    db_host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'

    data_loader = RedshiftDataLoader(db_user, db_pass, db_host)
    data_loader.load_data_to_database(openaq_df)


if __name__ == '__main__':
    main()