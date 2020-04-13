#%%
from fun_CreateTable import *
import pymysql
#import csv
import ast
import pandas as pd
import time
import itertools
host_name = "localhost"
username = "sugyeong"
password = "12341234"
database_name = "kt_db"

db = pymysql.connect(
    host=host_name,  # DATABASE_HOST
    port=3306,
    user=username,  # DATABASE_USERNAME
    passwd=password,  # DATABASE_PASSWORD
    db=database_name,  # DATABASE_NAME
    charset='utf8'
)
cursor = db.cursor()

sql = """
set session 
sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
"""
cursor.execute(sql)
db.commit()

#%%
################################################################################
######################## kt ####################################################
################################################################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/ULSAN_NG_201801.csv'
sql=createTable_stat(path, 'etl_ymd')
cursor.execute(sql)
db.commit()


#%%
# INSERT
for i in range(1,13):
    month = format(i, '02')
    sql1 = "LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_2018"+month+".csv"
    sql = sql1+"""
INTO TABLE kt_db.kt
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
    """
    print(sql)
    cursor.execute(sql)
    db.commit()


#%% 
# Check
sql = "SELECT count(*) FROM kt;"
df = pd.read_sql(sql, db)
print(df)



#%%
################################################################################
################### etl_ymd ####################################################
################################################################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/etl_ymd.csv'
sql=createTable_stat(path, 'etl_ymd')
cursor.execute(sql)
db.commit()


#%%
# MODIFY
sql = """
ALTER TABLE etl_ymd MODIFY etl_ymd_datetype datetime;
"""
cursor.execute(sql)
db.commit()

# MODIFY
sql = """
ALTER TABLE etl_ymd MODIFY holiday_nm varchar(10);
"""
cursor.execute(sql)
db.commit()


#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/etl_ymd.csv'
INTO TABLE kt_db.etl_ymd
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

#%%
# KEY
sql = "ALTER TABLE etl_ymd MODIFY COLUMN etl_ymd int(11) PRIMARY KEY"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT * FROM etl_ymd;"
df = pd.read_sql(sql, db)
print(df)
df.columns


#%%
################################################################################
################### etl_ymd_weather ############################################
################################################################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/etl_ymd_weather.csv'
sql=createTable_stat(path, 'etl_ymd_weather')
cursor.execute(sql)
db.commit()


#%%
# MODIFY
sql = """
ALTER TABLE etl_ymd_weather MODIFY etl_ymd_datetype datetime;
"""
cursor.execute(sql)
db.commit()

# MODIFY
sql = """
ALTER TABLE etl_ymd_weather MODIFY pm10_max varchar(10);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE etl_ymd_weather MODIFY pm10_min varchar(10);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE etl_ymd_weather MODIFY zungu_pm10_max varchar(10);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE etl_ymd_weather MODIFY zungu_pm10_min varchar(10);
"""
cursor.execute(sql)
db.commit()


#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/etl_ymd_weather.csv'
INTO TABLE kt_db.etl_ymd_weather
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()


#%%
# KEY
sql = "ALTER TABLE etl_ymd_weather MODIFY COLUMN etl_ymd int(11) PRIMARY KEY"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT zungu_pm10_min FROM etl_ymd_weather;"
df = pd.read_sql(sql, db)
print(df)
df.columns


#%%
###############################################
################### admi cd ###################
###############################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/admi_cd.csv'
sql=createTable_stat(path, 'admi_cd')
cursor.execute(sql)
db.commit()


#%%
# MODIFY
sql = """
ALTER TABLE admi_cd MODIFY admi_nm varchar(10);
"""
cursor.execute(sql)
db.commit()


#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/admi_cd.csv'
INTO TABLE kt_db.admi_cd
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()


#%%
# KEY
sql = "ALTER TABLE admi_cd MODIFY COLUMN admi_cd int(11) PRIMARY KEY"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT * FROM admi_cd;"
df = pd.read_sql(sql, db)
print(df)

#%%
################################################################################
############################### id_WGS #########################################
################################################################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/id_WGS.csv'
sql=createTable_stat(path, 'id_WGS')
#cursor.execute(sql)
#db.commit()


#%%
# MODIFY
sql = """
ALTER TABLE id_WGS MODIFY WGS_lat float(30,20);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE id_WGS MODIFY WGS_lon float(30,20);
"""
cursor.execute(sql)
db.commit()

#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/id_WGS.csv'
INTO TABLE kt_db.id_WGS_rn
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

#%%
# KEY
sql = "ALTER TABLE id_WGS MODIFY COLUMN id int(11) PRIMARY KEY"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT * FROM id_WGS;"
df = pd.read_sql(sql, db)
print(df)

################################################################################
############################### id_RN #########################################
################################################################################

# CREATE TABLE
path='/var/lib/mysql-files/kt/id_RN.csv'
sql=createTable_stat(path, 'id_RN')
cursor.execute(sql)
db.commit()


#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/id_RN.csv'
INTO TABLE kt_db.id_RN
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()


#%%
# KEY
sql = "ALTER TABLE id_RN MODIFY COLUMN id int(11) PRIMARY KEY"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT * FROM id_RN;"
df = pd.read_sql(sql, db)
df


# %%
################################################################################
############################### RN_geo #########################################
################################################################################

# CREATE TABLE
path = '/var/lib/mysql-files/kt/RN_geo.csv'
sql=createTable_stat(path, 'RN_geo')
cursor.execute(sql)
db.commit()


#%%
# MODIFY
sql = """
ALTER TABLE RN_geo MODIFY rn varchar(20);
"""
cursor.execute(sql)
db.commit()


#%%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/RN_geo.csv'
INTO TABLE kt_db.RN_geo
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()


#%%
# KEY
sql = "ALTER COLUMN geometry TYPE Geometry(LINESTRING, <SRID>) USING geometry(geom::Geometry, <SRID>)"
cursor.execute(sql)
db.commit()


#%% 
# Check
sql = "SELECT * FROM RN_geo;"
df = pd.read_sql(sql, db)
print(df)

