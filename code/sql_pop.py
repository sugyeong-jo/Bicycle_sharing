#%%
from fun_CreateTable import *
import pymysql
#import csv
import ast
import numpy as np
import pandas as pd
import geopandas as gpd
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
ZIP=gpd.read_file('/home/sugyeong/HDD/sugyeong/Python/kt/data/HangJeongDong_ver20190908.geojson')
zip_pop=pd.read_csv('/home/sugyeong/HDD/sugyeong/Python/kt/data/ULSAN_NG_Residence_201911_utf.csv')
#ZIP[ZIP['adm_nm'].str.contains('울산')].head()
ZIP['admi_cd']=np.nan
for i in range(len(ZIP)):
    ZIP['admi_cd'][i]=ZIP['adm_cd2'][i][0:-2]
zip_pop['admi_cd']=np.nan
ulsan_zip=ZIP[ZIP['adm_nm'].str.contains('울산')]
for i in range(len(zip_pop)):        
    target_zip=str(zip_pop['행정동'][i]).strip()
    zip_pop['admi_cd'][i]=ulsan_zip[ulsan_zip['adm_nm'].str.contains(target_zip)]['admi_cd']

zip_pop=pd.merge(ZIP,zip_pop,on='admi_cd').drop(['행정동','위도','경도','구전체 인구에 대한 구성비(%)','adm_cd2','adm_cd','OBJECTID'],axis=1)
zip_pop=zip_pop.rename(columns={'세대수(세대)': 'households', '인구수(명)': 'pop','남자인구수(명)':'m_pop','여자인구수(명)':'f_pop'})
max_value = sum(zip_pop['pop'])
zip_pop['pop_ratio'] =np.nan
for i in range(len(zip_pop['pop'])):
    zip_pop['pop_ratio'][i]=zip_pop['pop'][i]/max_value
zip_pop=gpd.GeoDataFrame(zip_pop)
zip_pop.sort_values(by='pop',ascending=False).to_csv("/var/lib/mysql-files/kt/zip_pop.csv",encoding='utf-8', index=False)

#%%
from fun_CreateTable import createTable_stat
path='/var/lib/mysql-files/kt/zip_pop.csv'
pd.read_csv(path)
sql=createTable_stat(path, 'zip_pop')
cursor.execute(sql)
db.commit()

sql = """
drop table zip_pop;
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE zip_pop Modify geometry varchar(10000);
"""
cursor.execute(sql)
db.commit()


sql = """
ALTER TABLE zip_pop Modify pop_ratio decimal(10,10);
"""
cursor.execute(sql)
db.commit()



# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/zip_pop.csv'
INTO TABLE kt_db.zip_pop
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

# KEY
sql = "ALTER TABLE zip_pop MODIFY COLUMN admi_cd decimal(10,0) PRIMARY KEY"
cursor.execute(sql)
db.commit()


# Check
sql = "SELECT * FROM zip_pop;"
df = pd.read_sql(sql, db)
print(df)
df.columns

#%%
##############################################################
# grid
##############################################################
#id-grid mapping
sql = "SELECT * FROM id_WGS;"
id_wgs_set = pd.read_sql(sql, db)

print(id_wgs_set)
id_geo=gpd.GeoDataFrame(id_wgs_set, geometry=gpd.points_from_xy(id_wgs_set.wgs_lon, id_wgs_set.wgs_lat))

#polygon
m_lon=id_wgs_set.wgs_lon
m_lat = id_wgs_set.wgs_lat
numcols, numrows = 50, 50
xi = np.linspace(m_lon.min(), m_lon.max(), numcols)
yi = np.linspace(m_lat.min(), m_lat.max(), numrows)
xi, yi = np.meshgrid(xi, yi)


from shapely.geometry import Polygon, LineString, Point
from shapely.ops import nearest_points

grid_geo=[]
for j in range(len(xi)-1):    
    for i in range(len(xi)-1):
        grid_geo.append([str(j).zfill(2)+str(i).zfill(2),Polygon([(xi[j][i],yi[j][i]),(xi[j+1][i],yi[j+1][i]),(xi[j+1][i+1],yi[j+1][i+1]),(xi[j][i+1],yi[j][i+1])])])
grid_geo=pd.DataFrame(grid_geo)
grid_geo.columns=['grid_id','geometry']
grid_geo=gpd.GeoDataFrame(grid_geo, geometry = 'geometry')
grid_geo.sort_values(by='grid_id',ascending=False).to_csv("/var/lib/mysql-files/kt/grid_geo.csv",encoding='utf-8', index=False)



from geopandas.tools import sjoin
id_grid=sjoin(id_geo, grid_geo, how='left', op = 'intersects')[['id','grid_id']]
print(id_grid)
id_grid.sort_values(by='grid_id',ascending=False).to_csv("/var/lib/mysql-files/kt/id_grid.csv",encoding='utf-8', index=False)

sql = """
drop table id_grid;
"""
cursor.execute(sql)
db.commit()


from fun_CreateTable import createTable_stat
path='/var/lib/mysql-files/kt/id_grid.csv'
pd.read_csv(path)
sql=createTable_stat(path, 'id_grid')
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE id_grid Modify grid_id INT(4) ZEROFILL UNSIGNED;
"""
cursor.execute(sql)
db.commit()


# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/id_grid.csv'
INTO TABLE kt_db.id_grid
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

# Check
sql = "SELECT * FROM id_grid;"
df = pd.read_sql(sql, db)
print(df)
df.columns



# %%
sql = """
drop table grid_geo;
"""
cursor.execute(sql)
db.commit()


path='/var/lib/mysql-files/kt/grid_geo.csv'
pd.read_csv(path)
sql=createTable_stat(path, 'grid_geo')
cursor.execute(sql)
db.commit()


sql = """
ALTER TABLE grid_geo Modify geometry varchar(10000);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE grid_geo Modify grid_id INT(4) ZEROFILL UNSIGNED;
"""
cursor.execute(sql)
db.commit()



# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/grid_geo.csv'
INTO TABLE kt_db.grid_geo
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

# KEY
sql = "ALTER TABLE grid_geo MODIFY COLUMN grid_id INT(4) ZEROFILL UNSIGNED PRIMARY KEY"
cursor.execute(sql)
db.commit()

# Check
sql = "SELECT * FROM grid_geo;"
df = pd.read_sql(sql, db)
print(df)
df.columns
