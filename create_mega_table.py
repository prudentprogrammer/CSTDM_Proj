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
  
def create_mega_table():
  logger = getLogging()
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  createTableQuery = r"""
  DROP TABLE IF EXISTS network.mega_table_with_routes;
  CREATE TABLE network.mega_table_with_routes
  (
    "A" integer,
    "B" integer,
    "R1" numeric,
    "R2" numeric,
    "R3" numeric,
    "R4" numeric,
    "R5" numeric,
    "R6" numeric,
    "R7" numeric,
    "R8" numeric,
    "R9" numeric,
    route_order text[]
  ); 
  """
  
  cursor.execute(createTableQuery)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Created Mega Table")
  
def populate_mega_table():
  logger = getLogging()

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  rangeString = r"""
  SELECT MIN("Node"), MAX("Node")
  FROM input.nodes_to_counties
  WHERE "TAZCentroid" = 1;
  """
  
  cursor.execute(rangeString)
  results = cursor.fetchall()
  minPoint = results[0][0]
  maxPoint = results[0][1]
  
  for i in range(minPoint, maxPoint+1):
    listOfTuples = []
    for j in range(i, maxPoint+1):
      if i == j:
        continue
      # Print on every 100th iteration
      if j % 100 == 0:
        print(i, j)
      # execute the shortest path query
      shortestPathQuery = r"""
      SELECT "RegionID", COALESCE(total_cost, 0) AS total_cost FROM
      (
        SELECT initcap("REGION") AS region, SUM(cost) AS total_cost
        FROM input.node_links
        INNER JOIN
        (
          SELECT source, target, network.edge_table.cost
          FROM pgr_dijkstra('SELECT id, source, target, cost FROM network.edge_table', %s, %s, false, false)
          INNER JOIN network.edge_table ON (id2 = id)
        ) AS R1
        ON (R1.source = "A") AND (R1.target = "B")
        GROUP BY region
        ) AS R2
        FULL JOIN input.regionid_to_regions
        ON ("Region" = region);
      """ % (str(i), str(j))
      
      '''
      routesQuery = r"""
      SELECT array_agg(region) AS "region_orders" FROM
      (
        SELECT initcap("REGION") AS region
        FROM input.node_links
        INNER JOIN
        (
          SELECT source, target, network.edge_table.cost
          FROM pgr_dijkstra('SELECT id, source, target, cost FROM network.edge_table', %s, %s, false, false)
          INNER JOIN network.edge_table ON (id2 = id)
        ) R1
        ON (R1.source = "A") AND (R1.target = "B")
        GROUP BY region
      ) R2
      """ % (str(i), str(j))
      '''
      
   
      try:
        cursor.execute(shortestPathQuery)
        # retrieve the vertices along with their regions  from the database
        records_regions = cursor.fetchall()
        
      # Vertex not found in database
      except psycopg2.Error:
        conn.rollback()
        continue
      '''
      try:
        cursor.execute(routesQuery)
        # retrieve the vertices along with their regions  from the database
        records_routes = cursor.fetchall()
      except:
        conn.rollback()
        continue
      '''

      listOfTuples.append((i,j,records_regions[0][1],records_regions[1][1], records_regions[2][1], records_regions[3][1], records_regions[4][1], records_regions[5][1], records_regions[6][1], records_regions[7][1], records_regions[8][1]))#, records_routes[0]))
      
    args_str = ','.join(['%s' for t in listOfTuples])
    insert_query = 'INSERT INTO network.mega_table_with_routes VALUES {0}'.format(args_str)
    cursor.execute(insert_query, listOfTuples)
    print(str(i) + "ith insert done")
    conn.commit()
    
  logger.info("*" * 15)
  logger.info("Populated Mega Table")
  
  
  
if __name__ == "__main__":
  create_mega_table()
  populate_mega_table()
  print("Done executing script.")