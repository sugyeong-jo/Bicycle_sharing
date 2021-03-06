#%%
##############################################################################
# Package load
##############################################################################

# for sql DB
import pymysql
import csv
import ast

# for data aggregation.
import numpy as np
import pandas as pd
import time
from time import sleep
from tqdm import tqdm, trange,tqdm_notebook
import datetime
import os
import warnings
warnings.filterwarnings('ignore')
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# for Geography
import geopandas as gpd
from geopy.distance import distance, lonlat
from shapely.geometry import Polygon, LineString, Point
from shapely.ops import nearest_points
import pyproj
from pyproj import Transformer
import pycrs 
import geoplot as gplt
import geoplot.crs as gcrs


# for data visualisation.
#import plotly_express as px
#import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.pylab as plt
import plotly
plotly.__version__
#import plotly.plotly as py
import cufflinks as cf 
cf.go_offline(connected=True)
cf.set_config_file(theme='polar')
import deckgljupyter.Layer as deckgl
#from geoband.API import *

# for pydeck
from deckgljupyter import Layer



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
##############################################################################
# # Data load
##############################################################################

start = time.time()
sql = """
select a.*,b.wgs_lat, b.wgs_lon
from id_WGS as b
right join (SELECT id, timezn_cd, total, admi_cd, etl_ymd  
FROM kt  
where etl_ymd = '20180101') as a
on  a.id = b.id;
"""
df = pd.read_sql(sql, db)
print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
print(df)

df_pop = df

#%%
##############################################################################
# geopandas
##############################################################################
gdf = gpd.GeoDataFrame(
    df_pop, geometry=gpd.points_from_xy(df_pop.wgs_lon, df_pop.wgs_lat))

gdf_time = gdf[gdf['timezn_cd']==12]
gdf_time = gdf_time.reset_index().drop(['index'],axis=1)
max_value = gdf_time['total'].max()
gdf_time['total_norm'] =np.nan
for i in range(len(gdf_time['total'])):
    gdf_time['total_norm'][i]=gdf_time['total'][i]/max_value
gdf_time

#fig, ax = plt.subplots(1, 1)
gdf_time.plot(column='total_norm',legend=True,cmap='OrRd',scheme='quantiles')
plt.show()
gdf_time[gdf_time['total_norm']>0.1]


# %%
##############################################################################
# pydeck
##############################################################################
#삽입 데이터
data=[]
for i in range(len(gdf_time)):
    if gdf_time['total_norm'][i]<0.25:
        level = 1
    elif 0.25<=gdf_time['total_norm'][i] and gdf_time['total_norm'][i] <0.5:
        level = 100
    elif 0.5<=gdf_time['total_norm'][i]  and gdf_time['total_norm'][i] <0.75:
        level = 200
    else :
        level = 300
    data.append({'position': [gdf_time['wgs_lon'][i],gdf_time['wgs_lat'][i]],
                 'values': gdf_time['total'][i],
                 'color': [48, gdf_time['total_norm'][i] * 255, gdf_time['total_norm'][i] * 255, 255],
                 'level': level})
data

# mbox 연결
access_token = 'pk.eyJ1IjoiZGFlZG9sIiwiYSI6ImNqZGpqbmpnYzFscm8yd245YXM5MWQxeGgifQ.ACxKlSjUthNpixmVX2faMw'
view_options = {
    'center': [129.246, 35.537],
    'zoom': 12,
    'bearing': 0,
    'pitch': 60,
    'style': 'mapbox://styles/mapbox/dark-v9',
    'access_token': access_token
}

m = Layer.Map(**view_options)
GC_layer = Layer.GridCellLayer(data,
                               cellSize=50,
                               getElevation='obj => obj.values',
                               getPosition='obj => obj.position',
                               getColor = 'obj=>obj.color'
                              )
m.add(GC_layer)
m.show()


# %%
##############################################################################
# grid
##############################################################################

# Check
sql = "SELECT * FROM grid_geo;"
grid_geo = pd.read_sql(sql, db)
print(grid_geo)



#%%
###########################################################################
#특정 시간!
###########################################################################
sql = """
select d.grid_id,c.timezn_cd ,c.total, d.geometry 
from grid_geo as d  
left join (select b.grid_id, a.timezn_cd, sum(total) as total
from id_grid as b
right join (SELECT id, timezn_cd, total  
FROM kt  
where etl_ymd = '20180101' and timezn_cd = 14) as a
on  a.id = b.id
group by b.grid_id) as c
on d.grid_id = c.grid_id
;
"""

df = pd.read_sql(sql, db)
print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
df_grid=df
df_grid["total"]=df_grid["total"].fillna(0)
print(df_grid)
from shapely import wkt
df_grid['geometry'] = df_grid['geometry'].apply(wkt.loads)
df_grid = gpd.GeoDataFrame(df_grid, geometry = 'geometry')
df_grid_total_time = df_grid

df_grid.plot(column='total',legend=True,cmap='OrRd', scheme='quantiles')
plt.show()

#%%
fig, ax = plt.subplots(1, figsize=(10, 6))
ax.axis('off')
title=str('The cell flow at the Jan., 1, 2018, at 2PM')
ax.set_title(title, fontdict={'fontsize': '16', 'fontweight' : '3'})
ax.annotate('Source: KT',xy=(0.1, .1),  xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=8, color='#555555')
df_grid.plot(column='total',legend=True,cmap='OrRd', scheme='quantiles',ax=ax,linewidth=0.8,edgecolor='0')
fig.savefig('./grid_20180101_14_quant.png', dpi=300)

fig, ax = plt.subplots(1, figsize=(10, 6))
ax.axis('off')
title=str('The cell flow at the Jan., 1, 2018, at 2PM')
ax.set_title(title, fontdict={'fontsize': '16', 'fontweight' : '3'})
ax.annotate('Source: KT',xy=(0.1, .1),  xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=8, color='#555555')
df_grid.plot(column='total',legend=True,cmap='OrRd',ax=ax,linewidth=0.8,edgecolor='0')
fig.savefig('./grid_20180101_14.png', dpi=300)

#%%
###########################################################################
# 모든 시간!
###########################################################################
sql = """
select d.grid_id,c.timezn_cd ,c.total, d.geometry 
from grid_geo as d  
right join (select b.grid_id, a.timezn_cd, sum(total) as total
from id_grid as b
right join (SELECT id, timezn_cd, total  
FROM kt  
where etl_ymd = '20180101') as a
on  a.id = b.id
group by b.grid_id, a.timezn_cd) as c
on d.grid_id = c.grid_id
;
"""

sql = """
select b.grid_id, a.timezn_cd, sum(total) as total
from id_grid as b
right join (SELECT id, timezn_cd, total  
FROM kt  
where etl_ymd = '20180101') as a
on  a.id = b.id
group by b.grid_id, a.timezn_cd
;
"""
start = time.time()
df = pd.read_sql(sql, db)
print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
df_grid=df
df_grid["total"]=df_grid["total"].fillna(0)
print(df_grid)
#df_grid_total_time = df_grid

 

max_total= max(df_grid['total'])

# %%
from shapely import wkt

for Time in range(0,24):
    grid_time = pd.merge(df_grid.loc[df_grid['timezn_cd']==Time][['grid_id', 'timezn_cd', 'total']], grid_geo, how = 'right',on = 'grid_id')
    grid_time["total"]=grid_time["total"].fillna(0)
    grid_time['geometry'] = grid_time['geometry'].apply(wkt.loads)
    grid_time = gpd.GeoDataFrame(grid_time, geometry = 'geometry')
    grid_time['total'][grid_time['grid_id']==4848] = max_total   
    #grid_time.plot(column='total',legend=True,cmap='OrRd', scheme='quantiles')

    fig, ax = plt.subplots(1, figsize=(10, 6))
    ax.axis('off')
    title=str('The cell flow at the Jan., 1, 2018, at '+ str(Time))
    ax.set_title(title, fontdict={'fontsize': '16', 'fontweight' : '3'})
    ax.annotate('Source: KT',xy=(0.1, .1),  xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=8, color='#555555')
    grid_time.plot(column='total',legend=True,cmap='OrRd', scheme='quantiles',ax=ax,linewidth=0.8,edgecolor='0')
    fig.savefig('../result/grid_quant_20180101_'+str(Time)+'.png', dpi=300)

    fig, ax = plt.subplots(1, figsize=(10, 6))
    ax.axis('off')
    title=str('The cell flow at the Jan., 1, 2018, at '+ str(Time))
    ax.set_title(title, fontdict={'fontsize': '16', 'fontweight' : '3'})
    ax.annotate('Source: KT',xy=(0.1, .1),  xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=8, color='#555555')
    grid_time.plot(column='total',legend=True,cmap='OrRd',ax=ax,linewidth=0.8,edgecolor='0')
    fig.savefig('../result/grid_20180101_'+str(Time)+'.png', dpi=300)


