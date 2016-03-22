import os

postgres = "postgres"

# Define databasename and user in PostgreSQL
pg_user = "postgres"
pg_password = "postgres"
pg_host = "localhost"
pg_database = "cstdm_db"
pg_port = 5432

#scenario_path = r'.\base_2040'
current_scenario = 'base_2010'

#methods
# possible options are : geographic, fiftyfifty, ecological, additive
methods = ['additive']
# Also generate interregional (between regions) output
interregions = True

LOGFILEPATH = "./logs/"