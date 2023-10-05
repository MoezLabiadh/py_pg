import os
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd


def connect_to_postgresql(host,dbname,user,password):
    """ Connects to a PostgreSQL database using psycopg2"""
    try:
        connection = psycopg2.connect(
            host= host,
            dbname= dbname,
            user= user,
            password= password
        )

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
                
    return connection



def connect_to_postgresql2 (user, password, host, dbname):
    """ Connects to a PostgreSQL database using SQLAlchemy"""
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')
    connection = engine.connect()
    
    return connection



if __name__ == '__main__':
    host= 'localhost'
    dbname= 'wc_data'
    user= os.getenv('pg_user')
    password= os.getenv('pg_pwd')

    #connection= connect_to_postgresql(host,dbname,user,password)
    connection= connect_to_postgresql(user, password, host, dbname)
    
    # test the connection
    sql= "SELECT name, zone FROM district_regional_areas"
    df= pd.read_sql(text(sql),connection)
