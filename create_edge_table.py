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

def create_edge_table():
  logger = getLogging()
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  createString = r"""
  DROP TABLE IF EXISTS network.edge_table;
  CREATE TABLE network.edge_table(
    id SERIAL,
    source int4,
    target int4,
    cost float8
  );
  """
  
  cursor.execute(createString)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Created Edge Table")
  
def populate_edge_table():
  logger = getLogging()
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  populateString = r"""
  INSERT INTO network.edge_table(id, source, target, cost)
  SELECT index, "A", "B", "DISTANCE"
  FROM input.node_links;
  """
  
  cursor.execute(populateString)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Populated Edge Table")
 
  indexString = r"""
  CREATE INDEX lookup_index
  ON network.edge_table (source, target);
  """
  
  cursor.execute(indexString)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Added Index on Edge Table for Lookup")
  
if __name__ == "__main__":
  create_edge_table()
  populate_edge_table()
  print("Done executing script.")