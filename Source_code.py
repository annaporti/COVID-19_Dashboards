# coding: utf-8
import pandas
import geopandas as gpd
from datetime import datetime

# 1. DATA LOADING
# ## 1.1 Load Geo-info
# Read locally stored shapefiles
gdf_shires  = gpd.read_file("admin/shires.geojson")
gdf_towns   = gpd.read_file("admin/towns.geojson")

# Clean GDFs, keep ony wanted rows
gdf_towns = gdf_towns[["codiine","nom_muni","municipi","geometry"]]
gdf_shires.drop("aft",axis=1,inplace=True)

# ## 1.2 Load and prepare Population Info
# Read population info from csv
pop_shires = pandas.read_csv("admin/pop_shires.csv",sep=';')
pop_towns = pandas.read_csv("admin/pop_towns.csv",sep=';')

# Drop column with empty fields
pop_shires = pop_shires.dropna(axis=0)
pop_towns = pop_towns.dropna(axis=0)

# Fix id: iterate over rows, if lenght is too short add a leading zero
ls = []
for i in pop_shires["ID"]:
    t = str(int(i))
    if len(t)==1:
        t = "0"+t
    ls.append(t)
pop_shires["ID"] = ls

# # Fix id: iterate over rows, if lenght is too short add a leading zero
ls = []
for i in pop_towns["Codi"]:
    t = str(int(i))
    if len(t)==5:
        t = "0"+t
    ls.append(t)
pop_towns["Codi"] = ls

# ## 1.3 Load all rows from Database storage and merge
# Define URLs for data retrieval
municipis_db_link = "https://analisi.transparenciacatalunya.cat/api/views/jj6z-iyrp/rows.csv?accessType=DOWNLOAD&sorting=true"
comarcas_db_link = "https://analisi.transparenciacatalunya.cat/api/views/c7sd-zy9j/rows.csv?accessType=DOWNLOAD&sorting=true"

# Load data from URL to pandas dataframe
df_towns_local = pandas.read_csv(municipis_db_link)
df_shires_local  = pandas.read_csv(comarcas_db_link)

# Rename columns
df_shires_local.columns  = ['nom', 'codi', 'data', 'sexe', 'grup_edat', 'residencia',
       'casos_confirmat', 'pcr', 'ingressos_total', 'ingressos_critic',
       'ingressats_total', 'ingressats_critic', 'exitus']
df_towns_local.columns   = ['data', 'comarcacodi', 'comarcadescripcio', 'municipicodi',
       'municipidescripcio', 'districtecodi', 'districtedescripcio',
       'sexecodi', 'sexedescripcio', 'resultatcoviddescripcio', 'numcasos']

# Convert date from string to datetime datatype
date_new = []
for i in df_shires_local["data"]:
    date_new.append(datetime.strptime(i, "%d/%m/%Y"))
df_shires_local["data"] = date_new

date_new = []
for i in df_towns_local["data"]:
    date_new.append(datetime.strptime(i, "%d/%m/%Y"))
df_towns_local["data"] = date_new

# Sort by date
df_shires  = df_shires_local.sort_values(by=['data'])
df_towns   = df_towns_local.sort_values(by=['data'])

# # 2. DATA CLEANING
# ## 2.1 Filter data
# Defining columns of interest
shires_cols  = ["codi","nom","data","casos_confirmat"]
towns_cols   = ["municipicodi","comarcacodi","municipidescripcio","data","numcasos"]

# Perform filtering - keep only aggregated all sexes in shires
df_shires  = df_shires[df_shires["sexe"]=="Tots"] # extract tots
df_shires  = df_shires[shires_cols]
df_towns   = df_towns[towns_cols]

# ## 2.2 Fix Code ine with leading zeroes
new_code = []
for i in df_towns["municipicodi"]:
    try:
        if i>0 and len(str(int(i)))==4:
            _ = str(int(i))
            _ = "0"+_
            new_code.append(_)
        else:
            new_code.append(str(int(i)))
    except ValueError:
        new_code.append(str(0))
df_towns["municipicodi"] = new_code

# ## 2.3 Calculate Shires as sum of towns
# Get unique data and shire codes
towns_comarca = df_towns["comarcacodi"].unique()
data_comarca = df_towns["data"].unique()

# Extract rows per shire and unique date
date_ls = []
counter = 0
for date in data_comarca:
    date_df = df_towns[df_towns["data"]==date]
    for mun in date_df["comarcacodi"].unique():
        try:
            val = sum(list(date_df[date_df["comarcacodi"]==mun]["numcasos"]))
        except IndexError:
            val = 0
        try:
            mun_ = str(int(mun))
            if len(mun_)!=2:
                mun_ = "0"+mun_
        except ValueError:
            mun_ = "00"
        
        date_ls.append([date,mun_,val])
    
    counter=counter+1
    if counter%10==0:
        print("progress: ",counter,"/",len(data_comarca),"          ",end="\r")
print("Done!                            ")

# Write result to DF
df_shires = pandas.DataFrame(date_ls,columns=["data","comarcacodi","numcasos"])

# ## 2.4. Fix df_towns
# Drop non classified
df_towns = df_towns.fillna(0)
a = df_towns.drop(df_towns[df_towns.municipidescripcio == "No classificat"].index)
a = a.drop(a[a.comarcacodi == 0].index)

# Convert float to string, add leading 0 if necessary
_ = []
for i in a["comarcacodi"]:
    __ = str(int(i))
    if len(__)==1:
        __ = "0"+__
    _.append(__)
a["comarcacodi"] = _
df_towns = a

# ## 2.5 Fix df_shires
# Drop where comarca 00
df_shires = df_shires.drop(df_shires[df_shires.comarcacodi == "00"].index)

# ## 2.6 Extract most recent Date
# ### 2.6.1 for towns
# Get uique towns, dates and extract most recent DF
un_towns = df_towns["municipicodi"].unique()
most_recent_date_towns = df_towns["data"].unique().max()
df_towns_most_recent = df_towns[df_towns["data"] == most_recent_date_towns]

# Merge most recent date df with geodataframe of towns
cases_most_recent_towns = gdf_towns.merge(df_towns_most_recent, left_on='codiine',right_on="municipicodi", how='outer', suffixes=('_l', '_r'))
cases_most_recent_towns["numcasos"] = cases_most_recent_towns["numcasos"].fillna(0)


# ### 2.6.2 for shires
# Get unique towns, dates and extract most recent DF
un_shires = df_shires["comarcacodi"].unique()
most_recent_date_shires = df_shires["data"].unique().max()
df_shires_most_recent = df_shires[df_shires["data"]==most_recent_date_shires]

# Merge most recent date df with geodataframe of towns
cases_most_recent_shires = gdf_shires.merge(df_shires_most_recent, left_on='comarca',right_on="comarcacodi", how='outer', suffixes=('_l', '_r'))
cases_most_recent_shires["numcasos"] = cases_most_recent_shires["numcasos"].fillna(0)

# ## 3. Calculate Incidence (/100k)
# Merge population data with cases
pop_towns = pop_towns[["Codi","Pop_total"]]
gdf_towns = gdf_towns.merge(pop_towns, left_on='municipi',right_on="Codi", how='inner', suffixes=('_l', '_r'))

pop_shires = pop_shires[["ID","Població"]]
gdf_shires = gdf_shires.merge(pop_shires, left_on='comarca',right_on="ID", how='outer', suffixes=('_l', '_r'))

# ## 3.1 Calculate
def calculate_incidence_shires(df,gdf):
    ls = []
    
    d = gdf[["comarca","Població"]]
    d = dict(zip(d.comarca, d.Població))
    
    counter = 0
    for code,cases in zip(df["comarcacodi"],df["numcasos"]):
        try:
            pop = d[code]
            incidence = round((100000/pop) * cases,2)
            ls.append(incidence)
        except KeyError:
            ls.append(0)
        counter= counter+1

    df["incidence"] = ls
    return(df)

df_shires = calculate_incidence_shires(df_shires,gdf_shires)

def calculate_incidence_towns(df,gdf):
    ls = []
    
    d = gdf[["codiine","Pop_total"]]
    d = dict(zip(d.codiine, d.Pop_total))
    
    counter = 0
    for code,cases in zip(df["municipicodi"],df["numcasos"]):
        #pop = int(list(gdf[gdf["codiine"]==code]["Pop_total"])[0])
        pop = d[code]
        incidence = round((100000/pop) * cases,2)
        ls.append(incidence)
        counter= counter+1
    df["incidence"] = ls
    return(df)

df_towns = calculate_incidence_towns(df_towns,gdf_towns)

# ## 3.2 Add comarcacodi
cases_most_recent_shires["comarcacodi"] = cases_most_recent_shires["comarca"]

# ## 3.3 Sync to PostGIS DB
from sqlalchemy import create_engine
import geoalchemy2
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon

# Drop/fill na for DB sync
cases_most_recent_shires = cases_most_recent_shires.fillna(0)
cases_most_recent_towns  = cases_most_recent_towns[cases_most_recent_towns['geometry'].notna()] # remove invalid geometries
cases_most_recent_towns  = cases_most_recent_towns.fillna(0) # fill NaNs with 0
# Create connector
engine = create_engine('postgresql://catalunya_bot:YRmT5XQcmqSaRQq2@85.214.150.208:5432/catalunya_covid')
print("connected to database!")
# Sync shires
cases_most_recent_shires.to_postgis('shires_map', engine ,if_exists='replace' ,dtype={'geom': Geometry(geometry_type='MULTIPOLYGON', srid= 4326)})
print("cases_most_recent_shires synced...")
# Sync towns
cases_most_recent_towns.to_postgis('towns_map', engine ,if_exists='replace' ,dtype={'geom': Geometry(geometry_type='MULTIPOLYGON', srid= 4326)})
print("cases_most_recent_towns synced...")
# Sync shires data
df_shires.to_sql('shires_covid', engine, if_exists='replace')
print("df_shires synced...")
# Sync towns data
df_towns.to_sql('towns_covid', engine, if_exists='replace')
print("cases_most_recent_towns synced...")
# Sync geodata
#gdf_shires.to_postgis('shires_geo', engine, if_exists='replace')
#gdf_towns.to_postgis('towns_geo', engine, if_exists='replace')
print("geodata synced...")
print("Done!")

# # Write to Log
import time
from datetime import datetime

# datetime object containing current date and time
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
latest_db_entry = list(cases_most_recent_shires["data"])[0]

f = open("log.txt", "a")
logstr = "Run at "+dt_string+", latest date in DB: "+str(latest_db_entry)+"\n"
f.write(logstr)
f.close()

