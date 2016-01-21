import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import pprint

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
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
      #print queryString
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
        
    finalTableName = cs.current_scenario + '_' + model + '_geographic_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    for i in range(len(countRegions) - 1):
      regionPercent = countRegions[i] / countRegions[-1]
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i + 1, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Geographic Final Tables")
  
def generateInterOnlyGeographicTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
      #print queryString
      cursor.execute(queryString)
      
      logging.info(cursor.statusmessage)
      conn.commit()
      
  logger.info("*" * 15)
  logger.info("Done creating Geographic Intermediate Tables")
  
  
  for model in tableList:
    allTablesForModel = tableList[model]
    countRegions = [0] * 10
    if model == 'ETM':
      continue
    for table in allTablesForModel:
      queryString = r"""
        SELECT * FROM process."%s";
      """ % (table)
      cursor.execute(queryString)
      records = cursor.fetchall()[0]
      # For the ETM case
      
      for index,item in enumerate(records):
        countRegions[index] += item
        
    finalTableName = cs.current_scenario + '_' + model + '_inter_geographic_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    for i in range(len(countRegions) - 1):
      regionPercent = countRegions[i] / countRegions[-1]
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i + 1, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Geographic Final Tables")
  
def generate5050Tables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
        
      print 'Now processing', tableName
      
      aggregationTableName = tableName.replace('_aggregation_table', '_fifty_intr_one')
      queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT origin_region, destination_region, "R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9" AS region_sum
      FROM network."%s";
      """ % (aggregationTableName, aggregationTableName, tableName)
      cursor.execute(queryString)
      conn.commit()
      
      queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT COALESCE(origin_region, 8) as region_ids, SUM(region_sum) as total_sum
      FROM
      (
        SELECT origin_region, region_sum
        FROM process."%s"
        UNION ALL
        SELECT destination_region, region_sum
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
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
  
def generate5050InterOnlyTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_fifty_intr_two')
      if 'SDPTM' in aggregationTableName:
        tableList['SDPTM'].append(aggregationTableName)
      elif 'LDPTM' in aggregationTableName and "Trips" in aggregationTableName:
        tableList['LDPTM'].append(aggregationTableName)
      elif 'SDCVM' in aggregationTableName:
        tableList['SDCVM'].append(aggregationTableName)
        
      print 'Now processing', tableName
      
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_fifty_intr_one')
      queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT origin_region, destination_region, "R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9" AS region_sum
      FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) temp;
      """ % (aggregationTableName, aggregationTableName, tableName)
      cursor.execute(queryString)
      conn.commit()
      
      queryString = r"""
      DROP TABLE IF EXISTS process."%s";
      CREATE TABLE process."%s" AS
      SELECT COALESCE(origin_region, 8) as region_ids, SUM(region_sum) as total_sum
      FROM
      (
        SELECT origin_region, region_sum
        FROM process."%s"
        UNION ALL
        SELECT destination_region, region_sum
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
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
  
  
def generateEcologicalTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Geographic Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
          SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
          FROM network."%s"
          GROUP BY "SerialNo", "Tour"
        ) R2
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
        
    finalTableName = cs.current_scenario + '_' + model + '_ecological_output'
    queryString = r"""
      DROP TABLE IF EXISTS output."%s";
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")
  
def parseSDPTM(sdptmTables):
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
    print 'Done with %s' % table
    
  print 'Done Parsing'
    
  
  
def generateInterEcologicalTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, applying Ecological Allocation method.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
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
     
      print 'Now processing', tableName
      
      aggregationTableName = tableName.replace('_aggregation_table', '_inter_ecological_intermediate')
      if 'SDPTM' in tableName:
        aggregationTableName = tableName.replace('_aggregation_table', '_inter_ecological_intr_one')
        tableList['SDPTM'].append(aggregationTableName)
        queryString = r"""
        DROP TABLE IF EXISTS process."%s";
        CREATE TABLE process."%s" AS
        SELECT 
        array_agg(COALESCE("R1",0) + COALESCE("R2",0) + COALESCE("R3",0) + COALESCE("R4",0) + COALESCE("R5",0) + COALESCE("R6",0) + COALESCE("R7",0) + COALESCE("R8",0) + COALESCE("R9",0)) AS region_sum , 
        array_agg("Leg"::text || ':' || destination_region::text) AS dest_region
        FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) temp
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
          FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) temp
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
          SELECT SUM("R1"+"R2"+"R3"+"R4"+"R5"+"R6"+"R7"+"R8"+"R9") AS region_sum, (array_agg(destination_region::int))[1] AS dest_region
          FROM (SELECT * FROM network."%s" WHERE origin_region != destination_region) temp
          GROUP BY "SerialNo", "Tour"
        ) R2
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
      CREATE TABLE output."%s"(countyid integer, percentages numeric);
    """ % (finalTableName, finalTableName)
    cursor.execute(queryString)
    conn.commit()
    
    total_sum = sum(list(countRegions.values()))
    for i in range(1, 10):
      regionPercent = countRegions[i] / total_sum
      queryString = r"""INSERT INTO output."%s" VALUES (%s, %s);""" % (finalTableName, i, regionPercent)
      cursor.execute(queryString)
      conn.commit()
 
  logger.info("*" * 15)
  logger.info("Done creating Ecological Final Tables")

        
if __name__ == "__main__":
  if 'geographic' in cs.methods:
    generateGeographicTables()
    if cs.interregions:
      generateInterOnlyGeographicTables()
      
  if 'fiftyfifty' in cs.methods:
    generate5050Tables()
    if cs.interregions:
        generate5050InterOnlyTables()
        
  if 'ecological' in cs.methods:
    generateEcologicalTables()
    if cs.interregions:
      generateInterEcologicalTables()
    
  

