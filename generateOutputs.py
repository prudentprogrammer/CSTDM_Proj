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

        
if __name__ == "__main__":
  #generateGeographicTables()
  #generateInterOnlyGeographicTables()
  #generate5050Tables()
  generate5050InterOnlyTables()
    
  

