import pymysql
import sqlalchemy as sqla
import os
import pandas as pd
import dotenv

# -- Funcion auxiliar: lee los commandos de los archivos .sql --
#se tienen que borrar los comentarios en los files sql
def ejecturarDesdeFile(file_name):
    sql_file = open(f'src/sql/{file_name}', 'r')
    contenido_sql = sql_file.read()
    sql_file.close()
    allStatements = contenido_sql.split(';') 

    for statement in allStatements:
        try:
            engine.execute(statement)
        except Exception as e:
            print("Se salteo el siguiente comando: ",e)




# 1) Connect to the database here using the SQLAlchemy's create_engine function

def connect():
    global engine # this allows us to use a global variable called engine
    # A "connection string" is basically a string that contains all the databse credentials together
    connection_string ="mysql+pymysql://lruco8yvvlr41r63:kpgv5561087z54r0@cxmgkzhk95kfgbq4.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ch3agl4hoedp83j4"
    print("Starting the connection...")
    engine = sqla.create_engine(connection_string)
    engine.connect()
    return engine

connect()

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

ejecturarDesdeFile("create.sql")

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

ejecturarDesdeFile("insert.sql")

# 4) Use pandas to print one of the tables as dataframes using read_sql function
sql_df = pd.read_sql("SELECT * FROM authors",engine)
print(sql_df)
#output
'''
   author_id first_name middle_name  last_name
0          1    Merritt        None       Eric
1          2      Linda        None        Mui
2          3     Alecos        None  Papadatos
3          4    Anthony        None   Molinaro
4          5      David        None     Cronin
5          6    Richard        None       Blum
6          7      Yuval        Noah     Harari
7          8       Paul        None     Albitz
'''