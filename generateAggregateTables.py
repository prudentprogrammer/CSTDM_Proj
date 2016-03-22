import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time

def getLogging():
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logger = logging.getLogger('edge logger')
  return logger

def generateAggregateTables():
  logger = getLogging()
  logger.info("*" * 15)
  logger.info("Now, creating Aggregation Tables.....")
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.pg_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.pg_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  # Take all the input files and generate aggregation tables
  createTableQuery = r"""
  SELECT table_name FROM information_schema.tables WHERE table_schema='input';
  """
  cursor.execute(createTableQuery)
  
  records = cursor.fetchall()
  
  for record in records:
    tableName = record[0]
    print 'Now processing', tableName
    if cs.current_scenario in tableName:
      if "SDPTM" in tableName:
        aggregationTableName = tableName + '_aggregation_table'
        queryString = r"""
          DROP TABLE IF EXISTS network."%s";
          CREATE TABLE network."%s" AS
          SELECT R3.*, "RegionID" as destination_region FROM
          (
            SELECT R2.*, "RegionID" as origin_region FROM
            (
              SELECT "SerialNo", "Person", "Tour", "Trip", "Leg", "TourPurp", "I", "J", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", route_orders FROM
              (SELECT * FROM input."%s" WHERE "TourMode" = 'SOV' OR "TourMode" = 'HOV2') R1
              INNER JOIN network.mega_table_with_routes
              ON "I"="A" AND "J"="B"
            ) R2
            LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) B
            ON ("I" = B."TAZ12")
          ) R3
          LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) C
          ON ("J" = C."TAZ12")
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        
        logging.info("SDPTM Aggregation Table %s created." % tableName)
        conn.commit()
        
      elif "LDPTM" in tableName:
        aggregationTableName = tableName + '_aggregation_table'
        
        if "Trips" in tableName:
          queryString = r"""
            DROP TABLE IF EXISTS network."%s";
            CREATE TABLE network."%s" AS
            
            SELECT R3.*, "RegionID" as destination_region FROM
            (
              SELECT R2.*, "RegionID" as origin_region FROM
              (
                SELECT "SerialNo", "Person", "Tour", "Trip", "Direction", "DPurp", "I", "J", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", route_orders FROM
                (
                  SELECT "SerialNo", (array_agg("Person"))[1] as "Person", (array_agg("Tour"))[1] as "Tour", (array_agg("Trip"))[1] as "Trip", (array_agg("Direction"))[1] as "Direction", (array_agg("DPurp"))[1] as "DPurp", (array_agg("I"))[1] as "I",  (array_agg("J"))[1] as "J"  FROM
                  (SELECT * FROM input."%s" WHERE "Mode" = 'SOV' OR "Mode" = 'HOV2' OR "Mode" = 'HOV3' OR "Mode" = 'CVR') R1
                  GROUP BY "SerialNo"
                ) one_person
                INNER JOIN network.mega_table_with_routes
                ON "I"="A" AND "J"="B"
              ) R2
              LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) B
              ON ("I" = B."TAZ12")
            ) R3
            LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) C
            ON ("J" = C."TAZ12")
          """ % (aggregationTableName, aggregationTableName, tableName)
          cursor.execute(queryString)
        
          logging.info("LDPTM Aggregation Table %s created." % tableName)
          conn.commit()
      
      elif "SDCVM" in tableName and "final" in tableName:
        aggregationTableName = (tableName + '_aggregation_table').replace('_final', '')
        queryString = r"""
          DROP TABLE IF EXISTS network."%s";
          CREATE TABLE network."%s" AS
          
          SELECT R3.*, "RegionID" as destination_region FROM
          (
            SELECT R2.*, "RegionID" as origin_region FROM
            (
               SELECT *
               FROM input."%s"
            ) R2
            LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) B
            ON ("I" = B."TAZ12")
          ) R3
          LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) C
          ON ("J" = C."TAZ12")
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        
        logging.info("SDCVM Aggregation Table %s created." % tableName)
        conn.commit()
        
      elif "LDCVM" in tableName:
        aggregationTableName = tableName + '_aggregation_table'
        queryString = r"""
          DROP TABLE IF EXISTS network."%s";
          CREATE TABLE network."%s" AS
          SELECT R3.*, "RegionID" as destination_region FROM
          (
            SELECT R2.*, "RegionID" as origin_region FROM
            (
             SELECT origin, destination, type, "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", route_orders FROM
            (SELECT * FROM input."%s") R1
            INNER JOIN network.mega_table_with_routes
            ON origin="A" AND destination="B"
            ) R2
            LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) B
            ON (origin = B."TAZ12")
          ) R3
          LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) C
          ON (destination = C."TAZ12")
          ;
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        
        logging.info("LDCVM Aggregation Table %s created." % tableName)
        conn.commit()
      
      elif "ETM" in tableName:
        aggregationTableName = tableName + '_aggregation_table'
        queryString = r"""
          DROP TABLE IF EXISTS network."%s";
          CREATE TABLE network."%s" AS
          
          SELECT R3.*, "RegionID" as destination_region FROM
          (
            SELECT R2.*, "RegionID" as origin_region FROM
            (
              SELECT "SerialNo", "ActorType", "Ext", "Int", "DPurp", "I", "J", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", route_orders FROM
              (SELECT * FROM input."%s") R1
              INNER JOIN network.mega_table_with_routes
              ON "I"="A" AND "J"="B"
            ) R2
            LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) B
            ON ("I" = B."TAZ12")
          ) R3
          LEFT JOIN (SELECT DISTINCT * FROM (SELECT "TAZ12", "Region", "RegionID" from input.nodes_with_regionids)S) C
          ON ("J" = C."TAZ12")
        """ % (aggregationTableName, aggregationTableName, tableName)
        cursor.execute(queryString)
        
        logging.info("ETM Aggregation Table %s created." % tableName)
        conn.commit()
        
      
  logger.info("*" * 15)
  logger.info("Done creating Aggregation Tables")

        
if __name__ == "__main__":
  generateAggregateTables()
  print "Done generating the aggregation tables"
    
  

