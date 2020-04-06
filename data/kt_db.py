#%%
import pymysql
import csv
import ast
import pandas as pd
import time
from itertools import islice
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


# %%
def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
            # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'decimal'
    else:
        return 'varchar'


def createTable_stat(path, tablename):
    f = open(path, 'r')
    reader = csv.reader(f)
    longest, headers, type_list = [], [], []
    print('--------The data is followed--------')
    for row in islice(reader, 0, 2) :
        print(row)
        if len(headers) == 0:
            headers = row
            for col in row:
                longest.append(0)
                type_list.append('')
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type
            if len(row[i]) > longest[i]:
                longest[i] = len(row[i])
    f.close()
    print('------------------------------------')        

    statement = 'create table '+tablename+' ('

    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

    statement = statement[:-1] + ');'
    print('--------The sql statement is followed--------')
    print(statement)
    print('---------------------------------------------')
    return statement


###########################################################################
################# kt table ################################################
###########################################################################
'''
# 잘 못 저장된 파일 수정
df=pd.read_csv('/var/lib/mysql-files/kt/ULSAN_NG_201802.csv')
del df["Unnamed: 0"]
print(df)
df.to_csv('/var/lib/mysql-files/kt/ULSAN_NG_201802.csv', index=False)

df=pd.read_csv('/var/lib/mysql-files/kt/ULSAN_NG_201803.csv')
del df["Unnamed: 0"]

print(df)
#df.to_csv('/var/lib/mysql-files/kt/ULSAN_NG_201803.csv', index=False)


df=pd.read_csv('/var/lib/mysql-files/kt/ULSAN_NG_201804.csv')
del df["Unnamed: 0"]
print(df)
df.to_csv('/var/lib/mysql-files/kt/ULSAN_NG_201804.csv', index=False)


df=pd.read_csv('/var/lib/mysql-files/kt/ULSAN_NG_201805.csv')
del df["Unnamed: 0"]
print(df)
df.to_csv('/var/lib/mysql-files/kt/ULSAN_NG_201805.csv', index=False)
'''

# %% 
# CREATE TABLE
path = '/var/lib/mysql-files/kt/ULSAN_NG_201801.csv'
statement = createTable_stat(path, 'kt')
cursor.execute(statement)
db.commit()


# %%
# INSERT data
print('201801')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201801.csv'
INTO TABLE kt_db.kt_test
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201802')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201802.csv'
INTO TABLE kt_db.kt_test
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201803')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201803.csv'
INTO TABLE kt_db.kt_test
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201804')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201804.csv'
INTO TABLE kt_db.kt_test
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201805')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201811.csv'
INTO TABLE kt_db.kt_test
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201806')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201806.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201807')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201807.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201808')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201808.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201809')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201809.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201810')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201810.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201811')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201811.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

print('201812')
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/ULSAN_NG_201812.csv'
INTO TABLE kt_db.kt_fin
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()


# %%
# CHEKING
sql = "SELECT * FROM kt limit 1;"
df = pd.read_sql(sql, db)
print(df)
df.columns

sql = "SELECT etl_ymd FROM kt_fin where CAST(etl_ymd as CHAR) LIKE '201801%' limit 2;"
df = pd.read_sql(sql, db)
print(df)

sql = "SELECT etl_ymd FROM kt limit 100;"
df = pd.read_sql(sql, db)
print(df)


######################################################################
###############id_WGS###################################################
########################################################################
# %%
from pyproj import Transformer

sql = "set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"
cursor.execute(sql)
db.commit()

sql = "SELECT id, x, y FROM kt group by id;"
df = pd.read_sql(sql, db)
print(df)

id_df= df.copy()

transproj_eq = Transformer.from_proj(
    '+proj=tmerc     +lat_0=38     +lon_0=128     +k=0.9999     +x_0=400000     +y_0=600000     +ellps=bessel     +towgs84=-115.8,474.99,674.11,1.16,-2.31,-1.63,6.43 +units=m +no_defs',
    'EPSG:4326',
    always_xy=True,
    skip_equivalent=True)

coor_array=id_df[['x','y']]
print(coor_array)

#계산
coor=[]
for i in range(len(id_df)):
    coor.append((coor_array['x'][i],coor_array['y'][i]))
WGS_list=[]
for pt in transproj_eq.itransform(coor):
    WGS=('{:.20f} {:.20f}'.format(*pt).split(' '))
    WGS_list.append((float(WGS[0]),float(WGS[1])))

WGS_lat=[]
WGS_lon=[]
print('end')
for i in WGS_list: 
    WGS_lon.append(i[0])
    WGS_lat.append(i[1])

id_df_2=pd.DataFrame(id_df['id'])
id_df_2['WGS_lat']=WGS_lon[0]
id_df_2['WGS_lon']=WGS_lon[0]

for i in range(len(id_df_2)):
    id_df_2['WGS_lon'][i]=WGS_lon[i]
    id_df_2['WGS_lat'][i]=WGS_lat[i]


id_df_2['WGS_lat']=id_df_2['WGS_lat'].round(20)
id_df_2['WGS_lon']=id_df_2['WGS_lon'].round(20)

# 저장
#id_df_2.to_csv("/var/lib/mysql-files/kt/id_WGS.csv", index=False)

# %%
# CRATE TABLE
path='/var/lib/mysql-files/kt/id_WGS.csv'
sql=createTable_stat(path, 'id_WGS')
cursor.execute(sql)
db.commit()

#%%
# MODIFY
sql = """
ALTER TABLE id_WGS MODIFY wgs_lat float(30,20);
"""
cursor.execute(sql)
db.commit()

sql = """
ALTER TABLE id_WGS MODIFY wgs_lon float(30,20);
"""
cursor.execute(sql)
db.commit()

# %%
# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/id_WGS.csv'
INTO TABLE kt_db.id_WGS
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()
#%%
# KEY
sql = "ALTER TABLE id_WGS ADD CONSTRAINT PK_id PRIMARY KEY (id)"
cursor.execute(sql)
db.commit()
# %%
# Check
sql = "SELECT * FROM id_WGS;"
df = pd.read_sql(sql, db)
print(df)

'''
sql = "drop table id_WGS;"
cursor.execute(sql)
db.commit()
cursor.fetchall()
'''

#################################################################################
# %%
# 시간당 총 인원
sql = "set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"
cursor.execute(sql)
db.commit()

sql = "SELECT id, timezn_cd, sum(total) FROM kt group by timezn_cd;"
df = pd.read_sql(sql, db)
print(df)

sql = "SELECT * FROM id_WGS;"
df1 = pd.read_sql(sql, db)
pd.merge(df, df1, how='left' , on = 'id').to_csv('total_by_time.csv',index=False)

########################################################
########### date #######################################
#######################################################
# %%
import pandas as pd
import datetime

start = time.time()
sql = "SELECT etl_ymd FROM kt group by etl_ymd;"
ymd_df = pd.read_sql(sql, db)
print(ymd_df)
print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간


ymd_df['etl_ymd_dateType'] = ymd_df['etl_ymd'].apply(str) # str로 바꾸기
fconvert = lambda x : datetime.datetime.strptime(x, "%Y%m%d").date() # datetype으로 바꾸기
ymd_df['etl_ymd_dateType'] = ymd_df['etl_ymd_dateType'].apply(fconvert) #날짜를 datetime 형식으로 변경
fweekday = lambda x : datetime.datetime.weekday(x) #날짜를 요일로 변경하는 함수(월요일:0 화요일:1 ~ 일요일:6)
ymd_df['weekday'] = ymd_df['etl_ymd_dateType'].apply(fweekday) #날짜를 요일로 변경
ymd_df.to_csv('/var/lib/mysql-files/kt/etl_ymd.csv', index=False)


# %%
# CREATE TABLE
path='/var/lib/mysql-files/kt/etl_ymd.csv'
sql=createTable_stat(path, 'etl_ymd')
cursor.execute(sql)
db.commit()

# MODIFY
sql = """
ALTER TABLE etl_ymd MODIFY etl_ymd_dateType datetime;
"""
cursor.execute(sql)
db.commit()

# INSERT
sql = """
LOAD DATA INFILE '/var/lib/mysql-files/kt/etl_ymd.csv'
INTO TABLE kt_db.etl_ymd
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
"""
cursor.execute(sql)
db.commit()

# KEY
sql = "ALTER TABLE etl_ymd ADD CONSTRAINT PK_etl_ymd PRIMARY KEY (etl_ymd)"
cursor.execute(sql)
db.commit()
 
# Check
sql = "SELECT * FROM etl_ymd;"
df = pd.read_sql(sql, db)
print(df)


#################################################################################
################### street ######################################################
#################################################################################
#%%
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, LineString, Point
from shapely.ops import nearest_points

crs='+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=GRS80 +units=m +no_defs'
transproj_eq = Transformer.from_proj(
    crs,
    'EPSG:4326',
    always_xy=True,
    skip_equivalent=True)

transproj_eq

street_file = "../data/street/TL_SPRD_MANAGE.shp"
street = gpd.read_file(street_file, encoding='euckr')
#street[['RN','RN_CD','geometry']]    

def line_to_coordinates(x):
    L=x.coords[:]
    lat=[]
    lon=[]
    for i in range(len(L)):
        lat.append(L[i][1])
        lon.append(L[i][0])
    return [(x,y) for x,y in zip(lon,lat)]

street['coordinates'] = np.nan
street['coordinates'] = street['geometry'].apply(line_to_coordinates) 
street_0= gpd.GeoDataFrame(street[['RN','RN_CD','coordinates']] )

WGS_street_list=[]

for i in range(len(street_0)):
    WGS_list=[]
    for pt in transproj_eq.itransform(street_0['coordinates'][i]):
        WGS=('{:.10f} {:.10f}'.format(*pt).split(' '))
        WGS_list.append((float(WGS[0]),float(WGS[1])))
    WGS_street_list.append(WGS_list)

geo_total=[]
for i in range(len(WGS_street_list)):
    geo=gpd.GeoDataFrame(index=[0], crs={'init': 'epsg:4326'}, geometry=[LineString(WGS_street_list[i])])
    geo_total.append([street_0['RN'][i],street_0['RN_CD'][i],geo['geometry']])

street_line=gpd.GeoDataFrame(geo_total)
street_line.columns=['RN','RN_CD','geometry']

#%%
sql = "SELECT * FROM id_WGS;"
df = pd.read_sql(sql, db)
id_set = list(set(df['id']))
print('making a wgs_set which is [id, WGS_lon, WGS_lat]')

wgs_set=[]
for i in id_set:
    wgs_set.append([i, df[df.id==i].iloc[0]['wgs_lon'],df[df.id==i].iloc[0]['wgs_lat']])
wgs_set=pd.DataFrame(wgs_set)
wgs_set.columns = ['id','wgs_lon', 'wgs_lat']



#%%
import tqdm
id_info=gpd.GeoDataFrame(wgs_set, geometry=gpd.points_from_xy(wgs_set.wgs_lon, wgs_set.wgs_lat))
#print(nearest_points(street_line.geometry[0][0],gdf_time_0_1.geometry[0])[0])
print('making a RN_CD_set which is [RN_CD]')
RN_CD_list=[]
id_list =[]
for j in tqdm(range(len(id_info))):
    p = id_info.geometry[j]
    id_list.append(int(id_info.id[j]))
    p_dist=[]
    for i in range(len(street_line)):
        line = street_line.geometry[i][0]
        p_dist.append([street_line.RN_CD[i],p.distance(line)])    
    a=pd.DataFrame(p_dist)
    a.columns=['RN_CD','dist']
    RNs=str(a[a.dist.isin([min(a['dist'])])]['RN_CD'].to_list()[0])
    RN_CD_list.append(RNs)
    

#dict_id_street=dict(zip(id_list,RN_CD_list))
