import cstdm_settings as cs
import psycopg2
import psycopg2.extras

conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

cursor.execute('SELECT * FROM node_sequences;')

# retrieve the records from the database
records = cursor.fetchall()

temp = []
for i in range(len(records) - 1):
  if (records[i][0] + 1) != (records[i+1][0]):
    print(str(records[i][0]) + " and next is " + str(records[i+1][0]))
    for j in range(records[i][0] + 1, records[i+1][0]):
      temp.append(j)
      
x = open('output_list.txt', 'w+')
x.write(str(temp))
x.close()
print(len(temp))