from dotenv import load_dotenv
import sqlalchemy as db
import os
import pandas as pd


env_path = '.env'
load_dotenv(dotenv_path=env_path)

def connect():
    connection_url = db.engine.URL.create(
                        drivername="clickhouse",
                        username=os.getenv("CH_USER"),
                        password=os.getenv("CH_PASSWORD"),
                        host=os.getenv("CH_HOST"),
                        database="study",
                    )
    return connection_url

def get_data(filename,db_name='metrics'):
    '''
    Runs the SQL filename specified and returns the output of the sql query as pandas dataframe
    '''
    connection_url = connect()
    engine = db.create_engine(connection_url)
    try:
        connection = engine.connect()
        with open(filename,'r') as f:
            read_query = f.read()
            # print (pd.DataFrame(connection.execute(read_query)))
            fetched_table = pd.DataFrame(connection.execute(read_query))
            print('Connected to database->', db_name , ' ->' ,filename )
            return fetched_table
    except Exception as e:
        print('Connection to database ->',filename,'failed')
        print(e)

def get_data_query(query):
    connection_url = connect()
    engine = db.create_engine(connection_url)
    connection = engine.connect()
    return pd.DataFrame(connection.execute(query))