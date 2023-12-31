import os
from sqlalchemy import create_engine, text
import pandas as pd


def connect_to_pgsql (user, password, host, dbname):
    """ Connects to a PostgreSQL database using SQLAlchemy"""
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')
    
    return engine


def df_to_pgsql(engine, df, table_name):
    """ Writes a dataframe to a pgsql table"""
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    df.to_sql(name= table_name, 
              con= engine, 
              if_exists= 'replace', 
              index= False)
    

def drop_pgsql_table(engine, table_name):
    """ Drops tables from PostgresSQL database"""
    connection= engine.connect()
    connection.execute(text(f'DROP TABLE {table_name}'))
    connection.commit()
    
    
    
    
if __name__ == '__main__':
    
    host= 'localhost'
    dbname= 'wc_data'
    user= os.getenv('pg_user')
    password= os.getenv('pg_pwd')

    engine= connect_to_pgsql(user, password, host, dbname)
    
    df= pd.read_excel('max_tenure_terms.xlsx')
    
    table_name= 'max_tenure_terms'
    
    df_to_pgsql(engine, df, table_name)
    
    drop_table= False
    
    if drop_table== True:
        drop_pgsql_table(engine, table_name)
