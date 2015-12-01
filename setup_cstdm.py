import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import glob, os
import pandas as pd
from sqlalchemy import create_engine

'''
columnNames = {
  'node_links': ['A', 'B','DISTANCE','COUNTY_1','REGION'],
  'node_links_old': ['A', 'B','DISTANCE','COUNTY_1','REGION'],
  'nodes_to_counties': ['Node','TAZ12','County','Region','TAZCentroid'],
  'regionid_to_regions': ['RegionID', 'Region']
}
'''

def createSchemas():
  schemas = ['input', 'network', 'output', 'process']
  
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logName = open(fullPath, 'w+')
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logging.info("**********************")
  logging.info("Creating the database schemas")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  for s in schemas:
    cursor.execute('CREATE SCHEMA IF NOT EXISTS ' + s)
    conn.commit()
    logging.info("Created schema " + s)
    
def importTables():
  #os.chdir("/input")
  engine = create_engine('postgresql://postgres:postgres@localhost/cstdm_db')
  filePath = ".\\input\\"
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logging.info("**********************")
  
  for file in os.listdir("./input"):
    fileName = file.replace(".csv", "")
    #df = pd.read_csv(filePath + file, header = True, names = columnNames[fileName], skiprows = 0)
    df = pd.read_csv(filePath + file)
    df.to_sql(fileName, engine, schema='input', if_exists='replace')
    logging.info("Importing the csv: " + fileName)
  
def test():
  logger = logging.getLogger()
  logger.info("Test")
  
if __name__ == "__main__":
  createSchemas()
  importTables()
  print("Done executing script.")
  
  
  