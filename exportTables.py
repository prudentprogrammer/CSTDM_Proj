import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import pprint
import os

def getLogging():
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logger = logging.getLogger('edge logger')
  return logger

def exportTables():
  outputPathFolders = [r".\output", r".\output\all_results", r".\output\interregional_results"]
  for folder in outputPathFolders:
    if not os.path.exists(folder):
      os.makedirs(folder)
  

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='output';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  
  for table in records:
    table = table[0]
    query = r"""
    SELECT "Region", ROUND(COALESCE(percentages, 0), 5) AS "Percentages"
    FROM output."%s"
    LEFT JOIN input.regionid_to_regions
    ON countyid = "RegionID"
    """ % (table)
    
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    fileName = ''
    if "inter" in table:
      fileName = r".\output\interregional_results\%s.csv" % table
    else:
      fileName = r".\output\all_results\%s.csv" % table

    with open(fileName, 'w+') as f:
      cursor.copy_expert(outputquery, f)
  
  conn.commit()
  print "Done exporting tables."




if __name__ == "__main__":
  exportTables()