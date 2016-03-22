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
  mainFolderName = r"outputs\output_%s" % cs.current_scenario
  outputPathFolders = [mainFolderName, mainFolderName + r"\all_results", mainFolderName + r"\interregional_results"]
  for folder in outputPathFolders:
    if not os.path.exists(folder):
      os.makedirs(folder)
  

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
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
    if cs.current_scenario in table:
      query = r"""
      SELECT "Region", ROUND(COALESCE(percentages, 0), 5) AS "Percentages", total_sum as "Total Sum"
      FROM output."%s"
      LEFT JOIN input.regionid_to_regions
      ON countyid = "RegionID"
      WHERE "Region" != 'Unknown'
      """ % (table)
      
      outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
      fileName = ''
      if "inter" in table:
        fileName = r".\%s\interregional_results\%s.csv" % (mainFolderName, table)
      else:
        fileName = r".\%s\all_results\%s.csv" % (mainFolderName, table)

      with open(fileName, 'w+') as f:
        cursor.copy_expert(outputquery, f)
  
  conn.commit()
  print "Done exporting tables."


if __name__ == "__main__":
  exportTables()