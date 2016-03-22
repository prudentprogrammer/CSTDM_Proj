from __future__ import generators    # needs to be at the top of your module
import psycopg2
import psycopg2.extras
import logging
import time
import pprint
import cstdm_settings as cs

def ResultIter(cursor, arraysize=100000):
  while True:
    results = cursor.fetchmany(arraysize)
    if not results:
      break
    for result in results:
      yield result

def adjustSDPTM():

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  # Use another cursor for inserting
  cursor2 = conn.cursor()

  # Take all the input files and generate aggregation tables
  getAllTablesQuery = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='input';
  """
  cursor.execute(getAllTablesQuery)

  records = cursor.fetchall()

  allSDCVMTables = []
  for record in records:
    tableName = record[0]
    print("Processing " + tableName)
    if "SDCVM" in tableName and cs.current_scenario in tableName and "final" not in tableName:
      aggregationTableName = tableName + '_intermediate_table_1'
      allSDCVMTables.append(aggregationTableName)
      createTableQuery = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT R2.*, "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9"
      FROM
      (
        SELECT "SerialNo", "Person", "Tour", generate_subscripts("J", 1) as "Trip", "I", unnest("J") as "J"
        FROM
        (
          SELECT "SerialNo", "Person", "Tour", (array_agg("I"))[1] as "I", array_agg("J") as "J"
          FROM (SELECT * FROM input."%s" WHERE "Mode" = 'H' OR "Mode" = 'I' OR "Mode" = 'L' OR "Mode" = 'M') R1
          GROUP BY "SerialNo", "Person", "Tour"
        ) R1
      ) R2
      INNER JOIN network.mega_table
      ON ("I" = "A") AND ("J" = "B")
      """ % (aggregationTableName, aggregationTableName, tableName)
      
      cursor.execute(createTableQuery)
      conn.commit()
      
  anotherList = []  
  for tableName in allSDCVMTables:
    print('Processing ' + tableName + ' intermediate 2')
    targetTable = tableName.replace('table_1', 'table_2')
    anotherList.append(targetTable)
    queryString = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s" AS
    SELECT process."%s".*, COALESCE(NULLIF("Direction"::text, ''), 'In') as "Direction"
    FROM process."%s"
    LEFT OUTER JOIN
    (
    SELECT "SerialNo", "Person", "Tour", MIN("Trip") as min_trip, 'Out' as "Direction"
    FROM
    (
      SELECT R1.*, "Trip" FROM
      ( 
        SELECT "SerialNo", "Person", "Tour", MAX("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8")as max_c
        FROM process."%s"
        GROUP BY "SerialNo", "Person", "Tour"
      ) R1
      INNER JOIN process."%s" as p
      ON R1."SerialNo" = p."SerialNo" AND max_c = ("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8")
    ) R3
    GROUP BY "SerialNo", "Person", "Tour"
    ) R4
    ON (R4."SerialNo" = process."%s"."SerialNo") 
    AND (R4."Person" = process."%s"."Person") 
    AND (R4."Tour" = process."%s"."Tour") AND 
    (R4.min_trip = process."%s"."Trip")
    ORDER BY "SerialNo", "Person", "Tour", "Trip";
    """ % (targetTable, targetTable, tableName,tableName,tableName,tableName,tableName,tableName,tableName,tableName) 
    cursor.execute(queryString)
    conn.commit()
    
  for tableName in anotherList:
    print('Processing ' + tableName + ' intermediate 3')
    targetTable = tableName.replace('table_2', 'table_3')
    queryString = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s" AS
    SELECT "SerialNo", "Person", "Tour", array_agg("Trip") as "Trip", array_agg("R1") as "R1", array_agg("R2") as "R2", array_agg("R3") as "R3", array_agg("R4") as "R4", array_agg("R5") as "R5", array_agg("R6") as "R6", array_agg("R7") as "R7", array_agg("R8") as "R8", array_agg("R9") as "R9", array_agg("I") as "I", array_agg("J") as "J", array_agg("Direction") as "Direction"
    FROM process."%s"
    GROUP BY "SerialNo", "Person", "Tour"
    ORDER BY "SerialNo", "Person", "Tour", "Trip"
    """ % (targetTable, targetTable, tableName) 
    cursor.execute(queryString)
    conn.commit()
    
  
  for tableName in allSDCVMTables:
    print('Processing ' + tableName)
    cursor2 = conn.cursor(name='super_cursor', withhold=True)
    tableName = tableName.replace('table_1', 'table_3')
    targetTable = tableName.replace('table_3', 'table_4').replace('_1', '')
    print('target = ' + targetTable)
    
    queryString = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s" ("SerialNo" bigint,
    "Person" bigint,
    "Tour" bigint,
    "Trip" integer,
    "R1" numeric,
    "R2" numeric,
    "R3" numeric,
    "R4" numeric,
    "R5" numeric,
    "R6" numeric,
    "R7" numeric,
    "R8" numeric,
    "R9" numeric,
    "I" bigint,
    "J" bigint,
    direction text);""" % (targetTable, targetTable)
    cursor.execute(queryString)
    conn.commit()
    
    
    queryString = r"""
    SELECT * FROM process."%s";
    """ % (tableName)
 
    cursor2.execute(queryString)
    
    while True:
      records = cursor2.fetchmany(50000)
      
      if not records:
        break

      listOfTuples = []

      directions = []
      for record in records:
        directions = record[-1]
        indexOut = directions.index('Out')
        for i in range(0, indexOut):
          directions[i] = 'Out'
      
        for i in range(len(directions)):
          listOfTuples.append( (int(record[0]), int(record[1]), int(record[2]),
          int(record[3][i]), float(record[4][i]), float(record[5][i]), 
          float(record[6][i]),float(record[7][i]),float(record[8][i]),float(record[9][i]),
          float(record[10][i]),float(record[11][i]), float(record[12][i]), int(record[13][i]), int(record[14][i]), directions[i]) )
            
        if len(listOfTuples) % 5000 == 0:
          queryString = r"""INSERT INTO process."%s" VALUES """ % (targetTable)
          cursor.execute( queryString + str(listOfTuples).strip('[]'))
          listOfTuples = []
          conn.commit()
    
    queryString = r"""INSERT INTO process."%s" VALUES """ % (targetTable)
    cursor.execute( queryString + str(listOfTuples).strip('[]'))
    listOfTuples = []
    conn.commit()
    targetTab = targetTable.replace('table_4', 'final').replace('_intermediate','')
    queryString = r"""
    DROP TABLE IF EXISTS input."%s";
    CREATE TABLE input."%s" AS 
    SELECT process."%s".*, route_orders
    FROM process."%s"
    INNER JOIN network.mega_table_with_routes
    ON ("I" = "A") AND ("J" = "B")
    """ % (targetTab, targetTab, targetTable, targetTable)
    cursor.execute(queryString)
    conn.commit()
    cursor2.close()
    
     
  
if __name__ == "__main__":
  adjustSDPTM()
