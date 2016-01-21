import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import glob, os
import pandas as pd
from sqlalchemy import create_engine

def getLogging():
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logger = logging.getLogger('setup logger')
  return logger
  
def createSchemas():
  # These are our schemas in the PostgreSQL database
  schemas = ['input', 'network', 'output', 'process']
  
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Creating the database schemas")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  # Create the schemas in the database
  for s in schemas:
    cursor.execute('CREATE SCHEMA IF NOT EXISTS ' + s)
    conn.commit()
    logging.info("Created schema " + s)
    
# This function imports the node link files
def importTables():
  
  # Get the connection to the database
  engine = create_engine('postgresql://postgres:postgres@localhost/cstdm_db')
  # Assume input to be in the origin root folder of the project
  filePath = "input"
  logger = getLogger()
  logger.info("*" * 15)
  
  # Use Pandas to automatically infer types and import the node link files
  for file in os.listdir("./input"):
    fileName = file.replace(".csv", "")
    df = pd.read_csv(filePath + file)
    df.to_sql(fileName, engine, schema='input', if_exists='replace')
    logger.info("Importing the csv: " + fileName)
  
  
if __name__ == "__main__":
  createSchemas()
  importTables()
  print("Done executing script.")
  
  
  