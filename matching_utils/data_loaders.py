"""
@author: Milena Bajic (DTU Compute)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2 # pip install psycopg2==2.7.7
from json import loads


def load_GM_data(GM_trip):
    
    # Set up connection
    print("\nConnecting to PostgreSQL database to load GM")
    conn = psycopg2.connect(database="..", user="..", password="..", host="..", port=5435)
    
    # Get GM measurements 
    print('Getting GM measurements')
    quory = 'SELECT "TS_or_Distance","T","lat","lon","message" FROM "Measurements"  WHERE ("FK_Trip"=\'{0}\' AND "T"= \'acc.xyz\') ORDER BY "TS_or_Distance" ASC'.format(GM_trip)
    cursor = conn.cursor()
    sql_data = pd.read_sql(quory, conn, coerce_float = True)
  
    # Load from json 
    sql_data['GM_Acceleration_z'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.z'])
    sql_data['GM_Acceleration_x'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.x'])
    sql_data['GM_Acceleration_y'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.y'])  
    sql_data['GM_Acceleration_full'] = np.sqrt(sql_data.GM_Acceleration_x.values**2 + sql_data.GM_Acceleration_y.values**2 + sql_data.GM_Acceleration_z.values**2)
    sql_data.drop(columns=['T','message'],inplace=True,axis=1)
    sql_data.reset_index(inplace=True, drop=True)
    
    # Rename columns
    for col in sql_data.columns:
        new_col = col if col.startswith('GM_')==True else 'GM_'+col
        sql_data.rename(columns={col:new_col},inplace=True)
        
    # Get information about the trip
    print('Getting trip information')
    quory = 'SELECT * FROM "Trips" WHERE "TripId"=\'{0}\''.format(GM_trip)
    cursor = conn.cursor()
    trips = pd.read_sql(quory, conn) 
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
    
    return sql_data, trips


def load_DRD_data(DRD_trip, lat_min, lat_max):
        
    # Set up connection
    print("\nConnecting to PostgreSQL database to load p79 DRD data")
    conn = psycopg2.connect(database="..", user="..", password="..", host="..", port=5435)
     
    # Execute quory: get sensor data
    print('Executing')
    quory = 'SELECT "DRDMeasurementId","TS_or_Distance","T","lat","lon","message" FROM "DRDMeasurements" WHERE ("FK_Trip"=\'{0}\' AND "lat"<={1} AND "lat">={2}) ORDER BY "TS_or_Distance" ASC'.format(DRD_trip, lat_max, lat_min)
    cursor = conn.cursor()
    sql_data = pd.read_sql(quory, conn, coerce_float = True)

    # Comma to dot, then to float, then sort also in pandas after conversion
    sql_data.TS_or_Distance = sql_data.TS_or_Distance.map(lambda row: float(row.replace(',','.')))
    sql_data.sort_values(by ='TS_or_Distance', inplace=True)
    
    # Take needed measurements 
    raw = sql_data[sql_data['T']=='raw data']
    prof = sql_data[sql_data['T']=='Profilometer']
    sql_data = raw.merge(prof,on=['TS_or_Distance','lat','lon'], suffixes=('_row','_prof'))
    sql_data.drop(columns=['T_prof','T_row','DRDMeasurementId_row','DRDMeasurementId_prof'],inplace=True,axis=1)
    
    # Extract from json
    sql_data['DRD_Velocity'] = sql_data.message_row.apply(lambda message: loads(message)['Hastighed'])
    sql_data['DRD_Acceleration'] = sql_data.message_row.apply(lambda message: loads(message)['Acceleration'])
    sql_data['DRD_Laser5'] = sql_data.message_prof.apply(lambda message: loads(message)['Laser5'])
    sql_data['DRD_Laser21'] = sql_data.message_prof.apply(lambda message: loads(message)['Laser21']) 
    
    sql_data.drop(columns=['message_prof','message_row'],inplace=True,axis=1)
    sql_data.reset_index(inplace=True, drop=True)
    
    # Rename columns
    for col in sql_data.columns:
        new_col = col if col.startswith('DRD_')==True else 'DRD_'+col
        sql_data.rename(columns={col:new_col},inplace=True)
   
    # Get information about the trip
    print('Getting trip information')
    quory = 'SELECT * FROM "Trips" WHERE "TripId"=\'{0}\''.format(DRD_trip)
    cursor = conn.cursor()
    trips = pd.read_sql(quory, conn) 
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
    
    return sql_data, trips

def filter_latlon(data, col_string, lat_min, lat_max, lon_min, lon_max):
    data = data[data[col_string+'_lat'].between(lat_min,lat_max)]
    data = data[data.GM_lon.between(lon_min,lon_max)]
    data.reset_index(inplace=True, drop=True)
    return data