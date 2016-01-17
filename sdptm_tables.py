import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time

conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

for i in range(1,20):

  createTableQuery = r"""
  DROP TABLE IF EXISTS input."base_2010_SDPTM_trips_%d";
  CREATE TABLE input."base_2010_SDPTM_trips_%d"
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
    "Sex" int,
    "Age" int
  );
  """ % (i,i)
  cursor.execute(createTableQuery)
  conn.commit()
  
  COPY_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
  
  fileName = r'D:\CSTDM_caltrans\cstdm\2010\SDPTM\trips_%d.csv' % i
  f = open(fileName, 'r')
  table_name = 'input."base_2010_SDPTM_trips_%d"' % i
  cursor.copy_expert(sql=COPY_STATEMENT % table_name, file=f)
  f.close()
  print('Done with %d-th trip file' % i)
  
  conn.commit()