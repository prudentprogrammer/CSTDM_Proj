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
      model = os.path.split(root)[1]
      
      for name in files:
        fileName = cs.current_scenario + '_' + model + '_' + name.replace(".csv", "")
        print 'Now trying to process %s' % fileName

        conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        createTableQuery = ''

        if model == 'SDCVM':
          createTableQuery = r"""
            DROP TABLE IF EXISTS input."%s";
            CREATE TABLE input."%s"
            (
              "Model" bigint,
              "SerialNo" bigint,
              "Person" bigint,
              "Trip" bigint,
              "Tour" bigint,
              "HomeZone" bigint,
              "ActorType" text,
              "OPurp" text,
              "DPurp" text,
              "I" bigint,
              "J" bigint,
              "Time" bigint,
              "Mode" text,
              "StartTime" double precision,
              "EndTime" double precision,
              "StopDuration" double precision,
              "TourType" text,
              "OriginalTimePeriod" text
            ) """ % (fileName, fileName)
            
        elif model == "LDPTM":
          if "Trips" in name:
            createTableQuery = r"""
            DROP TABLE IF EXISTS input."%s";
            CREATE TABLE input."%s"
            (
              "Model" bigint,
              "SerialNo" bigint,
              "Person" bigint,
              "Trip" bigint,
              "Tour" bigint,
              "HomeZone" bigint,
              "ActorType" text,
              "OPurp" text,
              "DPurp" text,
              "I" bigint,
              "J" bigint,
              "Time" bigint,
              "Mode" text,
              "OStation" bigint,
              "DStation" bigint,
              "AccMode" text,
              "EgrMode" text,
              "MainMode" text,
              "Duration" bigint,
              "Direction" text,
              "HHSize" bigint,
              "HHWks" bigint,
              "HHInc" double precision,
              "HHCars" text,
              "Dist" double precision
            ) """ % (fileName, fileName)
          else:
            continue
      
        elif model == 'SDPTM':
          createTableQuery = r"""
          DROP TABLE IF EXISTS input."%s";
          CREATE TABLE input."%s"
          (
            "Model" bigint,
            "SerialNo" bigint,
            "Person" bigint,
            "Trip" bigint,
            "Tour" bigint,
            "HomeZone" bigint,
            "ActorType" text,
            "OPurp" text,
            "DPurp" text,
            "I" bigint,
            "J" bigint,
            "Time" bigint,
            "Mode" text,
            "Leg" integer,
            "TourPurp" text,
            "Dist" double precision,
            "Own" text,
            "IncGrp" text,
            "Income_K" double precision,
            "HHSize" integer,
            "License" integer,
            "Grade" text,
            "TourMode" text,
            "DayPat" text,
            "Sex" integer,
            "Age" integer
          ) """ % (fileName, fileName)
          
        elif model == 'ETM':
          createTableQuery = r"""
          DROP TABLE IF EXISTS input."%s";
          CREATE TABLE input."%s"
          (
            "Model" bigint,
            "SerialNo" bigint,
            "Person" bigint,
            "Trip" bigint,
            "Tour" bigint,
            "HomeZone" bigint,
            "ActorType" text,
            "OPurp" text,
            "DPurp" text,
            "I" bigint,
            "J" bigint,
            "Time" bigint,
            "Mode" text,
            "Ext" text,
            "Int" text
          ) """ % (fileName, fileName)
          
        elif model == 'LDCVM':
          createTableQuery = r"""
          DROP TABLE IF EXISTS input."%s";
          CREATE TABLE input."%s"
          (
            origin bigint,
            destination bigint,
            type text,
            trips numeric
          );
          """ % (fileName, fileName)
          
        cursor.execute(createTableQuery)
        conn.commit()
        
        COPY_STATEMENT = """
          COPY %s FROM STDIN WITH
              CSV
              HEADER
              DELIMITER AS ','
          """
        
        csvFilePath = os.path.join(root, name)
        f = open(csvFilePath, 'r')
        table_name = 'input."%s"' % fileName
        try:
          cursor.copy_expert(sql=COPY_STATEMENT % table_name, file=f)
          print 'Successfully inserted %s into database' % fileName
        except Exception as e:
          print '%s failed' % fileName
          print e
        conn.commit()
        f.close()
          
        
if __name__ == "__main__":
  importTables()
  print "Done importing the scenario"
  