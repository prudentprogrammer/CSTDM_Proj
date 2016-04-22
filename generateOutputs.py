import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import pprint
import operator
import sys

def getLogging():
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logger = logging.getLogger('edge logger')
  return logger

def generateGeographicTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM', 'LDCVM', 'ETM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_geographic_intermediate')
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
      elif 'LDCVM' in aggregationTableName:
        tableList['LDCVM'].append(aggregationTableName)
      elif 'ETM' in aggregationTableName:
        tableList['ETM'].append(aggregationTableName)
        
      print 'Now processing', tableName

      aggregationTableName = tableName.replace('_aggregation_table', '_geographic_intermediate')
      queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT 
        SUM("R1") AS "R1 SUM",
        SUM("R2") AS "R2 SUM",
        SUM("R3") AS "R3 SUM",
        SUM("R4") AS "R4 SUM",
        SUM("R5") AS "R5 SUM",
        SUM("R6") AS "R6 SUM",
        SUM("R7") AS "R7 SUM",
        SUM("R8") AS "R8 SUM",
        SUM("R9") AS "R9 SUM",
        SUM("R1"+ "R2" + "R3" + "R4" + "R5" + "R6" + "R7" + "R8" + "R9") AS "TOTAL SUM"
        FROM network."%s";
      """ % (aggregationTableName, aggregationTableName, tableName)
      cursor.execute(queryString)
      
      logging.info(cursor.statusmessage)
      conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Geographic Intermediate Tables")
  
  
  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = [0] * 10
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()[0]
      for index,item in enumerate(records):
        #print str(index) + "," + str(records) + "," + table
        countRegions[index] += item
        
    finalTableName = cs.current_scenario + '_' + model + '_geographic_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    for i in range(len(countRegions) - 1):
      regionPercent = countRegions[i] / countRegions[-1]
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i + 1, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Geographic Final Tables")
  
def generateInterOnlyGeographicTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method for Interregional.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM', 'LDCVM', 'ETM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_geographic_intermediate')
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
      elif 'LDCVM' in aggregationTableName:
        tableList['LDCVM'].append(aggregationTableName)
      elif 'ETM' in aggregationTableName:
        tableList['ETM'].append(aggregationTableName)

      print 'Now processing', tableName
      queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT 
        SUM("R1") AS "R1 SUM",
        SUM("R2") AS "R2 SUM",
        SUM("R3") AS "R3 SUM",
        SUM("R4") AS "R4 SUM",
        SUM("R5") AS "R5 SUM",
        SUM("R6") AS "R6 SUM",
        SUM("R7") AS "R7 SUM",
        SUM("R8") AS "R8 SUM",
        SUM("R9") AS "R9 SUM",
        SUM("R1"+ "R2" + "R3" + "R4" + "R5" + "R6" + "R7" + "R8" + "R9") AS "TOTAL SUM"
        FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) temp;
      """ % (aggregationTableName, aggregationTableName, tableName)
      cursor.execute(queryString)
      
      logging.info(cursor.statusmessage)
      conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Geographic Intermediate Tables")
  
  
  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = [0] * 10

    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()[0]

      for index,item in enumerate(records):
        countRegions[index] += item
        
    finalTableName = cs.current_scenario + '_' + model + '_inter_geographic_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    for i in range(len(countRegions) - 1):
      regionPercent = countRegions[i] / countRegions[-1]
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i + 1, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Geographic Final Tables")
  
def generate5050Tables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying 50-50 Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_fifty_intr_two')
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)

      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)

      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
        
      print 'Now processing 50-50 step 1', tableName

      aggregationTableName = tableName.replace('_aggregation_table', '_fifty_intr_one')
      if 'SDPTM' in tableName or 'LDPTM' in tableName: #or  'SDCVM' in tableName:
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT origin_region, destination_region, "R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9" AS region_sum
        FROM (
          SELECT "SerialNo", "Person", "Tour",
            (array_agg("R1"))[1] AS "R1",
            (array_agg("R2"))[1] AS "R2",
            (array_agg("R3"))[1] AS "R3",
            (array_agg("R4"))[1] AS "R4" ,
            (array_agg("R5"))[1] AS "R5",
            (array_agg("R6"))[1] AS "R6",
            (array_agg("R7"))[1] AS "R7",
            (array_agg("R8"))[1] AS "R8",
            (array_agg("R9"))[1] AS "R9",
            (array_agg("origin_region"))[1] as origin_region, 
            (array_agg("destination_region"))[array_upper(array_agg(destination_region), 1)] as destination_region
         FROM network."%s" GROUP BY "SerialNo", "Person", "Tour") R1;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()
        
        print 'Step 2 intermediate fifty-fifty'
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(origin_region, 8) as region_ids, SUM(region_sum) as total_sum
        FROM
        (
          SELECT origin_region, region_sum / 2 AS region_sum
          FROM process."%s"
          UNION ALL
          SELECT destination_region, region_sum /2 AS region_sum
          FROM process."%s"
        ) R1
        GROUP BY origin_region
        ORDER BY origin_region;
        """ % (aggregationTableName.replace('one', 'two'), aggregationTableName.replace('one', 'two'),aggregationTableName, aggregationTableName)
        cursor.execute(queryString)
        conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Ecological Intermediate Tables")

   
  
  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      for (regionid, corr_sum) in records:
        countRegions[regionid] += corr_sum
        
    finalTableName = cs.current_scenario + '_' + model + '_fifty_fifty_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
def generate5050InterOnlyTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying 50-50 Allocation method for interregional.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName and "Fuel" not in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_fifty_intr_two')
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
        
      print 'Now processing step 1 fifty fifty inter', tableName
      if 'LDPTM' in tableName:
        aggregationTableName = tableName.replace('_aggregation_table', '_inter_fifty_intr_one')
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT origin_region, destination_region, "R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9" AS region_sum
        FROM
      (
        SELECT "SerialNo", "Person", "Tour",
            (array_agg("R1"))[1] AS "R1",
            (array_agg("R2"))[1] AS "R2",
            (array_agg("R3"))[1] AS "R3",
            (array_agg("R4"))[1] AS "R4" ,
            (array_agg("R5"))[1] AS "R5",
            (array_agg("R6"))[1] AS "R6",
            (array_agg("R7"))[1] AS "R7",
            (array_agg("R8"))[1] AS "R8",
            (array_agg("R9"))[1] AS "R9",
            (array_agg("origin_region"))[1] as origin_region, 
            (array_agg("destination_region"))[array_upper(array_agg(destination_region), 1)] as destination_region
         FROM network."%s"
         GROUP BY "SerialNo", "Person", "Tour") temp_table
      WHERE origin_region != destination_region;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()
        
        print 'Step two inter fiftyfifty '
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(origin_region, 8) as region_ids, SUM(region_sum) as total_sum
        FROM
        (
          SELECT origin_region, region_sum/2 as region_sum
          FROM process."%s"
          UNION ALL
          SELECT destination_region, region_sum/2 as region_sum
          FROM process."%s"
        ) R1
        GROUP BY origin_region
        ORDER BY origin_region;
        """ % (aggregationTableName.replace('one', 'two'), aggregationTableName.replace('one', 'two'),aggregationTableName, aggregationTableName)
        cursor.execute(queryString)
        conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Ecological Intermediate Tables")

  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      for (regionid, corr_sum) in records:
        countRegions[regionid] += corr_sum
        
    finalTableName = cs.current_scenario + '_' + model + '_inter_fifty_fifty_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
  
  
def generateEcologicalTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName:
     
      print 'Now processing ' +  tableName + ' step 1'
      
      aggregationTableName = tableName.replace('_aggregation_table', '_ecological_intermediate')
      if 'SDPTM' in tableName:
        aggregationTableName = tableName.replace('_aggregation_table', '_ecological_intr_one')
        tableList['SDPTM'].append(aggregationTableName)

        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT 
        array_agg(COALESCE("R1",0) + COALESCE("R2",0) + COALESCE("R3",0) + COALESCE("R4",0) + COALESCE("R5",0) + COALESCE("R6",0) + COALESCE("R7",0) + COALESCE("R8",0) + COALESCE("R9",0)) AS region_sum , 
        array_agg("Leg"::text || ':' || destination_region::text) AS dest_region
        FROM network."%s"
        GROUP BY "SerialNo", "Person", "Tour";
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()
      elif 'LDPTM' in tableName:
        tableList['LDPTM'].append(aggregationTableName)

        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(dest_region, 8), SUM(region_sum) AS total_sum_region FROM
        (
          SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
          FROM network."%s"
          GROUP BY "SerialNo", "Person", "Tour"
        ) R2
        GROUP BY dest_region;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()
      elif 'SDCVM' in tableName:
        tableList['SDCVM'].append(aggregationTableName)
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(dest_region, 8), SUM(region_sum) AS total_sum_region FROM
        (
          SELECT * FROM
          (
            SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
            FROM network."%s"
            WHERE direction = 'Out'
            GROUP BY "SerialNo", "Tour", direction
          ) temp1
          UNION ALL
          SELECT * FROM
          (
            SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(origin_region::int))[1] AS dest_region
            FROM network."%s"
            WHERE direction = 'In'
            GROUP BY "SerialNo", "Tour", direction
          ) temp2
        ) R2
        GROUP BY dest_region;
        """ % (aggregationTableName, aggregationTableName, tableName, tableName)
        cursor.execute(queryString)
        conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Ecological Intermediate Tables")

  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    if 'SDPTM' in model:
      # conn.cursor will return a cursor object, you can use this cursor to perform queries
      sdptmTabs = list(tableList['SDPTM'])
      parseSDPTM(sdptmTabs)
      allTablesForModel = [x.replace('one', 'three') for x in sdptmTabs]
      
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      for (regionid, corr_sum) in records:
        countRegions[regionid] += corr_sum
        
    finalTableName = cs.current_scenario + '_' + model + '_ecological_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
def parseSDPTM(sdptmTables):
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  
  for table in sdptmTables:
    cursor = conn.cursor(name='super_cursor', withhold=True)
    cursor2 = conn.cursor()
    targetTable = table.replace('one', 'two')
    query = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s"(region integer, region_sum numeric);
    """ % (targetTable, targetTable)
    cursor2.execute(query)
    conn.commit()
    
    cursor.execute('SELECT * FROM process."%s"' % table)
    while True:
      rows = cursor.fetchmany(10000)
      
      if not rows:
        break

      listOfTuples = []
      for record in rows:
        distances = record[0]
        region_with_legs = record[1]
        region_with_legs = [x for x in region_with_legs if x != None]
        legs = [ x.split(':')[0] for x in region_with_legs]
        regions = [ x.split(':')[1] for x in region_with_legs]
        if len(legs) == 0:
          continue
        else:
          if '0' not in legs:
            individualRow = (regions[0], sum(distances))
            listOfTuples.append(individualRow)
          else:
            zero_index = legs.index('0')
            if len(legs) == 1 or zero_index == len(legs) - 1:
              firstRow = (regions[zero_index], sum(distances[:zero_index+1]))
              listOfTuples.append(firstRow)
            else:
              firstRow = (regions[zero_index], sum(distances[:zero_index+1]))
              secondRow = (regions[zero_index + 1], sum(distances[zero_index+1:]))
              listOfTuples.append(firstRow)
              listOfTuples.append(secondRow)
       

     
      listOfTuples = [ (int(region), float(region_sum)) for (region, region_sum) in listOfTuples]
      args_str = str(listOfTuples).strip('[]')
      insertQuery = 'INSERT INTO process."%s" '%(targetTable)
      insertQuery += ' VALUES ' + args_str
      cursor2.execute(insertQuery)
      conn.commit()
      
    query = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s" AS
    SELECT COALESCE(region, 8), SUM(region_sum) as region_sum
    FROM process."%s"
    GROUP BY region;
    """ % (targetTable.replace('two', 'three'), targetTable.replace('two', 'three'), targetTable)
    cursor2.execute(query)
    conn.commit()
    
    cursor.close()
    print 'Done parsing %s for SDPTM' % table
    
  print 'Done Parsing'
    
  
def generateInterEcologicalTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Ecological Allocation method for interregional.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName: #and "Fuel" not in tableName:
     
      print 'Now processing step 1 for inter ecological', tableName
      
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_ecological_intermediate')
      if 'SDPTM' in tableName:
        aggregationTableName = tableName.replace('_aggregation_table', '_inter_ecological_intr_one')
        tableList['SDPTM'].append(aggregationTableName)

        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT region_sum, dest_region
        FROM 
		    (
           SELECT "SerialNo", "Person", "Tour",
           array_agg("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum,
            array_agg("Leg"::text || ':' || destination_region::text) AS dest_region,
            (array_agg("origin_region"))[1] as origin_region, 
            (array_agg("destination_region"))[array_upper(array_agg(destination_region), 1)] as destination_region
             FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) inter_trips
             GROUP BY "SerialNo", "Person", "Tour"
        ) temp_table 
        
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()

      elif 'LDPTM' in tableName:
        tableList['LDPTM'].append(aggregationTableName)

        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(dest_region, 8), SUM(region_sum) AS total_sum_region FROM
        (
          SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
          FROM (
            SELECT "SerialNo", "Person", "Tour",
            SUM("R1") AS "R1",
            SUM("R2") AS "R2",
            SUM("R3") AS "R3",
            SUM("R4") AS "R4",
            SUM("R5") AS "R5",
            SUM("R6") AS "R6",
            SUM("R7") AS "R7",
            SUM("R8") AS "R8",
            SUM("R9") AS "R9",
            (array_agg("origin_region"))[1] as origin_region, 
            (array_agg("destination_region"))[array_upper(array_agg(destination_region), 1)] as destination_region
             FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) inter_trips GROUP BY "SerialNo", "Person", "Tour"
			     ) temp_table 
           GROUP BY "SerialNo", "Person", "Tour"
        ) final_table
     
        GROUP BY dest_region;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()

      elif 'SDCVM' in tableName:
        tableList['SDCVM'].append(aggregationTableName)

        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT COALESCE(dest_region, 8), SUM(region_sum) AS total_sum_region FROM
        (
            SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
            FROM (
             SELECT "SerialNo", "Person", "Tour",
              SUM("R1") AS "R1",
              SUM("R2") AS "R2",
              SUM("R3") AS "R3",
              SUM("R4") AS "R4",
              SUM("R5") AS "R5",
              SUM("R6") AS "R6",
              SUM("R7") AS "R7",
              SUM("R8") AS "R8",
              SUM("R9") AS "R9",
              (array_agg("origin_region"))[1] as origin_region, 
              (array_agg("destination_region"))[array_upper(array_agg(destination_region), 1)] as destination_region
               FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) inter_trips
               GROUP BY "SerialNo", "Person", "Tour"
            ) temp_table
              GROUP BY "SerialNo", "Person", "Tour"
         ) temp1
         GROUP BY dest_region;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        conn.commit()

  logger.info("*" * 15)
  logger.info("Done creating Ecological Intermediate Tables")

   
  
  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    if 'SDPTM' in model:
      # conn.cursor will return a cursor object, you can use this cursor to perform queries
      sdptmTabs = list(tableList['SDPTM'])
      parseSDPTM(sdptmTabs)
      allTablesForModel = [x.replace('one', 'three') for x in sdptmTabs]
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      for (regionid, corr_sum) in records:
        countRegions[regionid] += corr_sum
        
    finalTableName = cs.current_scenario + '_' + model + '_inter_ecological_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
def generateAdditiveTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Additive Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  tablesName = []
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_additive_intermediate_one')
      queryString = ''
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
        
      print 'Now processing', tableName
      queryString = ''
      if 'SDPTM' in aggregationTableName or ('LDPTM' in aggregationTableName and "Trips" in aggregationTableName) or 'SDCVM' in aggregationTableName:
      #if 'SDCVM' in aggregationTableName and "TH_AM" not in aggregationTableName:
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT (array_agg("I"))[1] as "I", 
        (array_agg("J"))[array_upper(array_agg("J"), 1)] as "J" ,
          SUM("R1") AS "R1 SUM",
          SUM("R2") AS "R2 SUM",
          SUM("R3") AS "R3 SUM",
          SUM("R4") AS "R4 SUM",
          SUM("R5") AS "R5 SUM",
          SUM("R6") AS "R6 SUM",
          SUM("R7") AS "R7 SUM",
          SUM("R8") AS "R8 SUM",
          SUM("R9") AS "R9 SUM",
          region_array_uniq(array_cat_agg(route_orders)) as route_orders, 
          (array_agg(origin_region))[1] as origin_region, 
          (array_agg(destination_region))[array_upper(array_agg(destination_region), 1)] as destination_region,
          "SerialNo", "Person", "Tour"
        FROM network."%s"
        GROUP BY "SerialNo", "Person", "Tour"
        """ % (aggregationTableName, aggregationTableName, tableName)
      else:
        continue
        
      cursor.execute(queryString)
      conn.commit()
      
  cursor.close()
  #sys.exit(0)
  
  for table in tablesName:
    cursor = conn.cursor(name='super_cursor', withhold=True)
    cursor2 = conn.cursor()
    targetTable = table.replace('one', 'two')
    query = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s"("I" bigint, "J" bigint, "R1 SUM" numeric, "R2 SUM" numeric,"R3 SUM" numeric,"R4 SUM" numeric,"R5 SUM" numeric,"R6 SUM" numeric,"R7 SUM" numeric,"R8 SUM" numeric,"R9 SUM" numeric, origin_region bigint, destination_region bigint, "SerialNo" bigint, "Person" bigint, "Tour" bigint);
    """ % (targetTable, targetTable)
    cursor2.execute(query)
    conn.commit()
    
    cursor.execute('SELECT * FROM process."%s"' % table)
    while True:
      rows = cursor.fetchmany(100000)
      
      if not rows:
        break

      listOfTuples = []
      # row[0] - i
      # row[1] - j
      # row[2-10] - gl_sum
      # row[11] - route_orders
      # row[12] - origin_region
      # row[13] - dest_region
      # row[14,15,16] - SerialNo, Person, Tour
      
      for row in rows:
        gl_sum = []
        
        for i in range(2, 10+1):
          gl_sum.append(row[i])
          
        ordered_routes = row[11]
        ordered_routes = [int(i) for i in ordered_routes]
        corresponding_sum = []

        for i in ordered_routes:
          corresponding_sum.append(gl_sum[i - 1])
        
        cumsum_list = list(accumulate(corresponding_sum))
        row_sum = sum(gl_sum)
        additive_sum = sum(cumsum_list)
        
       
        final_gl_list = [0]*9
        temp = 0
        for i in ordered_routes:
          final_gl_list[i - 1] = (cumsum_list[temp] * row_sum) / additive_sum
          temp = temp + 1     

        final_gl_list = list(map(float, final_gl_list))
        listOfTuples.append((int(row[0]), int(row[1]), final_gl_list[0], final_gl_list[1], final_gl_list[2], final_gl_list[3], final_gl_list[4], final_gl_list[5], final_gl_list[6], final_gl_list[7], final_gl_list[8], int(row[12]), int(row[13]), int(row[14]), int(row[15]), int(row[16])  ))
       

      args_str = str(listOfTuples).strip('[]')
      insertQuery = 'INSERT INTO process."%s" '%(targetTable)
      insertQuery += ' VALUES ' + args_str
      cursor2.execute(insertQuery)
      conn.commit()
      
   
    cursor.close()
    print 'Done with %s' % table
  
  cursor.close()
  cursor = conn.cursor()
  
  for table in tablesName:
    targetTable = table.replace('one', 'three')
    previousTable = table.replace('one', 'two')
    aggregation_table = table.replace('_additive_intermediate_one', '_aggregation_table')
    #print "Table = " + aggregation_table
    queryString = r"""
    DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      
      SELECT
      SUM("R1 SUM") AS "R1 SUM",
      SUM("R2 SUM") AS "R2 SUM",
      SUM("R3 SUM") AS "R3 SUM",
      SUM("R4 SUM") AS "R4 SUM",
      SUM("R5 SUM") AS "R5 SUM",
      SUM("R6 SUM") AS "R6 SUM",
      SUM("R7 SUM") AS "R7 SUM",
      SUM("R8 SUM") AS "R8 SUM",
      SUM("R9 SUM") AS "R9 SUM"
    
      FROM
      
      process."%s";
    """ % (targetTable, targetTable, previousTable)
      
      
    '''
    queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      
      SELECT
      SUM("R1_Adj") AS "R1 SUM",
      SUM("R2_Adj") AS "R2 SUM",
      SUM("R3_Adj") AS "R3 SUM",
      SUM("R4_Adj") AS "R4 SUM",
      SUM("R5_Adj") AS "R5 SUM",
      SUM("R6_Adj") AS "R6 SUM",
      SUM("R7_Adj") AS "R7 SUM",
      SUM("R8_Adj") AS "R8 SUM",
      SUM("R9_Adj") AS "R9 SUM"
    
      FROM
      (
        SELECT
        (("R1 SUM" / additive_sum) * vmt_sum) AS "R1_Adj",
        (("R2 SUM" / additive_sum) * vmt_sum) AS "R2_Adj",
        (("R3 SUM" / additive_sum) * vmt_sum) AS "R3_Adj",
        (("R4 SUM" / additive_sum) * vmt_sum) AS "R4_Adj",
        (("R5 SUM" / additive_sum) * vmt_sum) AS "R5_Adj",
        (("R6 SUM" / additive_sum) * vmt_sum) AS "R6_Adj",
        (("R7 SUM" / additive_sum) * vmt_sum) AS "R7_Adj",
        (("R8 SUM" / additive_sum) * vmt_sum) AS "R8_Adj",
        (("R9 SUM" / additive_sum) * vmt_sum) AS "R9_Adj"
      
        FROM
        (
          SELECT * FROM
          (
            SELECT "SerialNo", "Person", "Tour", (array_agg("R1"))[1] AS "R1",  (array_agg("R2"))[1] AS "R2 ",  (array_agg("R3"))[1] AS "R3",  (array_agg("R4"))[1] AS "R4",  (array_agg("R5"))[1] AS "R5",  (array_agg("R6"))[1] AS "R6",  (array_agg("R7"))[1] AS "R7",  (array_agg("R8"))[1] AS "R8",  (array_agg("R9"))[1] AS "R9", SUM(sum) as vmt_sum, (array_agg(origin_region))[1] AS origin_region, (array_agg(destination_region))[1] AS destination_region  FROM
            (
              SELECT "SerialNo", "Person", "Tour", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", origin_region, destination_region, SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9")
              OVER (PARTITION BY  "SerialNo", "Person", "Tour",  "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", origin_region, destination_region)
              FROM network."%s"
            )temp
          GROUP BY "SerialNo", "Person", "Tour"
          ) R1
          NATURAL INNER JOIN
          (
            SELECT "SerialNo", "Person", "Tour", (array_agg("R1 SUM"))[1] AS "R1 SUM",  (array_agg("R2 SUM"))[1] AS "R2 SUM", (array_agg("R3 SUM"))[1] AS "R3 SUM", (array_agg("R4 SUM"))[1] AS "R4 SUM", (array_agg("R5 SUM"))[1] AS "R5 SUM", (array_agg("R6 SUM"))[1] AS "R6 SUM", (array_agg("R7 SUM"))[1] AS "R7 SUM", (array_agg("R8 SUM"))[1] AS "R8 SUM", (array_agg("R9 SUM"))[1] AS "R9 SUM", SUM(sum) as additive_sum FROM
            (
              SELECT "SerialNo", "Person", "Tour", "R1 SUM", "R2 SUM", "R3 SUM", "R4 SUM", "R5 SUM", "R6 SUM", "R7 SUM", "R8 SUM", "R9 SUM", SUM("R1 SUM"+"R2 SUM"+"R3 SUM"+"R4 SUM"+"R5 SUM"+"R6 SUM"+"R7 SUM"+"R8 SUM"+"R9 SUM")
              OVER (PARTITION BY  "SerialNo", "Person", "Tour" , "R1 SUM", "R2 SUM", "R3 SUM", "R4 SUM", "R5 SUM", "R6 SUM", "R7 SUM", "R8 SUM", "R9 SUM")
              FROM process."%s"
            ) R2
            GROUP BY "SerialNo", "Person", "Tour"
          ) R3
        ) final_table_1
      ) final_table_2
      
      """ % (targetTable, targetTable, aggregation_table, previousTable)
    '''
    cursor.execute(queryString)
    print 'Done processing ' + previousTable
    conn.commit()
  

  for model in tableList:
    allTablesForModel = tableList[model]
    allTablesForModel = [x.replace('one', 'three') for x in allTablesForModel]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    
    for table in allTablesForModel:
      print 'Now processing ' + table
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      
      for (index, item) in enumerate(records[0]):
        countRegions[index + 1] += item
        
      finalTableName = cs.current_scenario + '_' + model + '_additive_output'
      queryString = r"""
        DROP TABLE IF EXISTS output."%s";
        CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
      """ % (finalTableName, finalTableName)
      cursor.execute(queryString)
      conn.commit()
      
      total_sum = sum(list(countRegions.values()))
      for i in range(1, 10):
        regionPercent = countRegions[i] / total_sum
        queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
        cursor.execute(queryString)
        conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Additive Final Tables")
    
    
  print 'Done Parsing'

  logger.info("*" * 15)
  logger.info("Done creating Additive Intermediate Tables")
  
def generateInterAdditiveTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Additive Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  getAllTablesString = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='network';
  """
  cursor.execute(getAllTablesString)
  
  records = cursor.fetchall()
  tableList = {}
  tablesName = []
  for model in ['SDPTM', 'LDPTM', 'SDCVM']:
    tableList[model] = []

  for record in records:
    tableName = record[0]
    if cs.current_scenario in tableName: #and "Fuel" not in tableName:
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_additive_intermediate_one')
      queryString = ''
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
        tablesName.append(aggregationTableName)
        
      print 'Now processing for Inter additive Step 1', tableName
      queryString = ''
      if 'SDPTM' in aggregationTableName or ('LDPTM' in aggregationTableName and "Trips" in aggregationTableName) or 'SDCVM' in aggregationTableName:
      #if 'SDCVM' in aggregationTableName and "TH_AM" not in aggregationTableName:
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
		SELECT * FROM
		(
        SELECT (array_agg("I"))[1] as "I", 
        (array_agg("J"))[array_upper(array_agg("J"), 1)] as "J" ,
          SUM("R1") AS "R1 SUM",
          SUM("R2") AS "R2 SUM",
          SUM("R3") AS "R3 SUM",
          SUM("R4") AS "R4 SUM",
          SUM("R5") AS "R5 SUM",
          SUM("R6") AS "R6 SUM",
          SUM("R7") AS "R7 SUM",
          SUM("R8") AS "R8 SUM",
          SUM("R9") AS "R9 SUM",
          region_array_uniq(array_cat_agg(route_orders)) as route_orders, 
          (array_agg(origin_region))[1] as origin_region, 
          (array_agg(destination_region))[array_upper(array_agg(destination_region), 1)] as destination_region,
          "SerialNo",
          "Person",
          "Tour"
        FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) inter_trips
        GROUP BY "SerialNo", "Person", "Tour" 
		) temp_table
        """ % (aggregationTableName, aggregationTableName, tableName)
      else:
        continue
        
      cursor.execute(queryString)
      conn.commit()
  #sys.exit(0)    

  
  for table in tablesName:
    cursor = conn.cursor(name='super_cursor', withhold=True)
    cursor2 = conn.cursor()
    targetTable = table.replace('one', 'two')
    query = r"""
    DROP TABLE IF EXISTS process."%s";
    CREATE TABLE process."%s"("I" bigint, "J" bigint, "R1 SUM" numeric, "R2 SUM" numeric,"R3 SUM" numeric,"R4 SUM" numeric,"R5 SUM" numeric,"R6 SUM" numeric,"R7 SUM" numeric,"R8 SUM" numeric,"R9 SUM" numeric, origin_region bigint, destination_region bigint, "SerialNo" bigint, "Person" bigint, "Tour" bigint);
    """ % (targetTable, targetTable)
    cursor2.execute(query)
    conn.commit()
    
    cursor.execute('SELECT * FROM process."%s"' % table)
    while True:
      rows = cursor.fetchmany(100000)
      
      if not rows:
        break

      listOfTuples = []
      # row[0] - i
      # row[1] - j
      # row[2-10] - gl_sum
      # row[11] - route_orders
      # row[12] - origin_region
      # row[13] - dest_region
      
      for row in rows:
        gl_sum = []
        
        for i in range(2, 10+1):
          gl_sum.append(row[i])
          
        ordered_routes = row[11]
        ordered_routes = [int(i) for i in ordered_routes]
        corresponding_sum = []

        for i in ordered_routes:
          corresponding_sum.append(gl_sum[i - 1])
        
        cumsum_list = list(accumulate(corresponding_sum))
        row_sum = sum(gl_sum)
        additive_sum = sum(cumsum_list)
        
       
        final_gl_list = [0]*9
        temp = 0
        for i in ordered_routes:
          final_gl_list[i - 1] = (cumsum_list[temp] * row_sum) / (additive_sum)
          temp = temp + 1     

        final_gl_list = list(map(float, final_gl_list))
        #final_gl_list = [x for x in final_gl_list]
        listOfTuples.append((int(row[0]), int(row[1]), final_gl_list[0], final_gl_list[1], final_gl_list[2], final_gl_list[3], final_gl_list[4], final_gl_list[5], final_gl_list[6], final_gl_list[7], final_gl_list[8], int(row[12]), int(row[13]), int(row[14]), int(row[15]), int(row[16])  ))
       

      args_str = str(listOfTuples).strip('[]')
      insertQuery = 'INSERT INTO process."%s" '%(targetTable)
      insertQuery += ' VALUES ' + args_str
      cursor2.execute(insertQuery)
      conn.commit()
      
   
    cursor.close()
    print 'Done with %s' % table
  
  cursor.close()
  cursor = conn.cursor()
  print "Done with step 2"

  
  for table in tablesName:
    targetTable = table.replace('one', 'three')
    previousTable = table.replace('one', 'two')
    aggregation_table = table.replace('_inter_additive_intermediate_one', '_aggregation_table')
    #print "Table = " + aggregation_table

    #if SDCVM" in 
    '''
    queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      
      SELECT
      SUM("R1_Adj") AS "R1 SUM",
      SUM("R2_Adj") AS "R2 SUM",
      SUM("R3_Adj") AS "R3 SUM",
      SUM("R4_Adj") AS "R4 SUM",
      SUM("R5_Adj") AS "R5 SUM",
      SUM("R6_Adj") AS "R6 SUM",
      SUM("R7_Adj") AS "R7 SUM",
      SUM("R8_Adj") AS "R8 SUM",
      SUM("R9_Adj") AS "R9 SUM"
    
      FROM
      (
        SELECT
        (("R1 SUM" / additive_sum) * vmt_sum) AS "R1_Adj",
        (("R2 SUM" / additive_sum) * vmt_sum) AS "R2_Adj",
        (("R3 SUM" / additive_sum) * vmt_sum) AS "R3_Adj",
        (("R4 SUM" / additive_sum) * vmt_sum) AS "R4_Adj",
        (("R5 SUM" / additive_sum) * vmt_sum) AS "R5_Adj",
        (("R6 SUM" / additive_sum) * vmt_sum) AS "R6_Adj",
        (("R7 SUM" / additive_sum) * vmt_sum) AS "R7_Adj",
        (("R8 SUM" / additive_sum) * vmt_sum) AS "R8_Adj",
        (("R9 SUM" / additive_sum) * vmt_sum) AS "R9_Adj"
      
        FROM
        (
          SELECT * FROM
          (
            SELECT "SerialNo", "Person", "Tour", (array_agg("R1"))[1] AS "R1",  (array_agg("R2"))[1] AS "R2 ",  (array_agg("R3"))[1] AS "R3",  (array_agg("R4"))[1] AS "R4",  (array_agg("R5"))[1] AS "R5",  (array_agg("R6"))[1] AS "R6",  (array_agg("R7"))[1] AS "R7",  (array_agg("R8"))[1] AS "R8",  (array_agg("R9"))[1] AS "R9", SUM(sum) as vmt_sum, (array_agg(origin_region))[1] AS origin_region, (array_agg(destination_region))[1] AS destination_region  FROM
            (
              SELECT "SerialNo", "Person", "Tour", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", origin_region, destination_region, SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9")
              OVER (PARTITION BY  "SerialNo", "Person", "Tour",  "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", origin_region, destination_region)
              FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) inter_trips
            )temp
          GROUP BY "SerialNo", "Person", "Tour"
          ) R1
          NATURAL INNER JOIN
          (
            SELECT "SerialNo", "Person", "Tour", sum("R1 SUM") AS "R1 SUM",  sum("R2 SUM") AS "R2 SUM", sum("R3 SUM") AS "R3 SUM", sum("R4 SUM") AS "R4 SUM", sum("R5 SUM") AS "R5 SUM", sum("R6 SUM") AS "R6 SUM", sum("R7 SUM") AS "R7 SUM", sum("R8 SUM") AS "R8 SUM", sum("R9 SUM") AS "R9 SUM", SUM(sum) as additive_sum FROM
            (
              SELECT "SerialNo", "Person", "Tour", "R1 SUM", "R2 SUM", "R3 SUM", "R4 SUM", "R5 SUM", "R6 SUM", "R7 SUM", "R8 SUM", "R9 SUM", SUM("R1 SUM"+"R2 SUM"+"R3 SUM"+"R4 SUM"+"R5 SUM"+"R6 SUM"+"R7 SUM"+"R8 SUM"+"R9 SUM")
              OVER (PARTITION BY  "SerialNo", "Person", "Tour" , "R1 SUM", "R2 SUM", "R3 SUM", "R4 SUM", "R5 SUM", "R6 SUM", "R7 SUM", "R8 SUM", "R9 SUM")
              FROM process."%s"
            ) R2
            GROUP BY "SerialNo", "Person", "Tour"
          ) R3
        ) final_table_1
      ) final_table_2
      
      """ % (targetTable, targetTable, aggregation_table, previousTable)
    '''
    
    queryString = r"""
    DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT
      SUM("R1 SUM") AS "R1 SUM",
      SUM("R2 SUM") AS "R2 SUM",
      SUM("R3 SUM") AS "R3 SUM",
      SUM("R4 SUM") AS "R4 SUM",
      SUM("R5 SUM") AS "R5 SUM",
      SUM("R6 SUM") AS "R6 SUM",
      SUM("R7 SUM") AS "R7 SUM",
      SUM("R8 SUM") AS "R8 SUM",
      SUM("R9 SUM") AS "R9 SUM"
    
      FROM
      
      process."%s";
    """ % (targetTable, targetTable, previousTable)
    cursor.execute(queryString)
    print 'Done processing ' + previousTable
    conn.commit()
    

  
  for model in tableList:
    allTablesForModel = tableList[model]
    allTablesForModel = [x.replace('one', 'three') for x in allTablesForModel]
    countRegions = {}
    for i in range(1, 10):
      countRegions[i] = 0
    
    for table in allTablesForModel:
      print 'Now processing ' + table
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()
      
      for (index, item) in enumerate(records[0]):
        countRegions[index + 1] += item
        
      finalTableName = cs.current_scenario + '_' + model + '_inter_additive_output'
      queryString = r"""
        DROP TABLE IF EXISTS output."%s";
        CREATE TABLE output."%s"(countyid integer, percentages numeric, total_sum numeric);
      """ % (finalTableName, finalTableName)
      cursor.execute(queryString)
      conn.commit()
      
      total_sum = sum(list(countRegions.values()))
      for i in range(1, 10):
        regionPercent = countRegions[i] / total_sum
        queryString = r"""INSERT INTO output."%s" VALUES (%s, %s, %s);""" % (finalTableName, i, regionPercent, countRegions[i])
        cursor.execute(queryString)
        conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Additive Interregional Final Tables")
    
    
  print 'Done Parsing'

  logger.info("*" * 15)
  logger.info("Done creating Additive Interregional Tables")
  


def accumulate(iterable, func=operator.add):
    'Return running totals'
    # accumulate([1,2,3,4,5]) --> 1 3 6 10 15
    # accumulate([1,2,3,4,5], operator.mul) --> 1 2 6 24 120
    it = iter(iterable)
    try:
        total = next(it)
    except StopIteration:
        return
    yield total
    for element in it:
        total = func(total, element)
        yield total
 

        
if __name__ == "__main__":
  if 'geographic' in cs.methods:
    generateGeographicTables()
    if cs.interregions:
      generateInterOnlyGeographicTables()
      
  if 'fiftyfifty' in cs.methods:
    #generate5050Tables()
    if cs.interregions:
        generate5050InterOnlyTables()
        
  if 'ecological' in cs.methods:
    #generateEcologicalTables()
    if cs.interregions:
      generateInterEcologicalTables()
      
  if 'additive' in cs.methods:
    #generateAdditiveTables()
    if cs.interregions:
      generateInterAdditiveTables()
    
  

