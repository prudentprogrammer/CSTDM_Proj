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
  logger = logging.getLogger('edge logger')
  return logger

# Imports the appropriate folder containing the scenario  
def importTables():
  logger = getLogging()
  engine = create_engine('postgresql://postgres:postgres@localhost/cstdm_db')
  
  for root, dirs, files in os.walk(cs.current_scenario):
    # Don't take the root directory
    if len(files) != 0:
      scenario = os.path.split(root)[0]
      model = os.path.split(root)[1]
      
      for name in files:
        fileName = scenario + '_' + model + '_' + name.replace(".csv", "")
        try:
          df = pd.read_csv(os.path.join(root, name))
          df.to_sql(fileName, engine, schema='input', if_exists='replace')
          logger.info("Importing the csv: " + fileName + " from the folder " + model)
        except Exception as e:
          print('Failed to import the csv ' + fileName)
          logging.info('Failed to import the csv ' + fileName)

        
if __name__ == "__main__":
  importTables()
  