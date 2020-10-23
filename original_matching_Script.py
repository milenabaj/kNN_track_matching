"""
@author: Milena Bajic (DTU Compute)
"""
import os, sys, glob, pickle
import pandas as pd
import numpy as np
from scipy.spatial import KDTree
import matplotlib.pyplot as plt
import psycopg2 # pip install psycopg2==2.7.7
from json import loads
from pandas.io.json import json_normalize #package for flattening json in pandas df
from sklearn.neighbors.dist_metrics import DistanceMetric

pd.set_option('precision', 12)
from sklearn.metrics.pairwise import haversine_distances 
# conda install -c conda-forge/label/gcc7 haversine 
from haversine import haversine

def load_obd_GM():
    # Set up connection
    print("\nConnecting to PostgreSQL database to load GM")
    conn = psycopg2.connect(database="postgres", user="mibaj", password="Vm9jzgBH", host="liradbdev.compute.dtu.dk", port=5432)
    
    # Get GM data
    print('Getting GM data')
    quory = 'SELECT "message" FROM "Measurements" WHERE ("FK_Trip"=\'f6fda7dd-5965-40ed-9cbd-da0b4beed457\' AND "T"=\'obd.spd_veh\')'
    cursor = conn.cursor()
    sql_data = pd.read_sql(quory, conn, coerce_float = True)
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
        
    sql_data["obd.spd_veh.value"] =  sql_data.message.apply(lambda message: loads(message)['obd.spd_veh.value'])
    sql_data.drop(columns=['message'],inplace=True,axis=1)
    return sql_data


def load_GM_data(GM_trip = '9d056547-7872-4ae4-8fbe-3c61214c4c00'):
    
    # Set up connection
    print("\nConnecting to PostgreSQL database to load GM")
    #conn = psycopg2.connect(database="postgres", user="mibaj", password="Vm9jzgBH", host="liradbdev.compute.dtu.dk", port=5432)
    conn = psycopg2.connect(database="postgres", user="mibaj", password="mibajLira123", host="liradb.compute.dtu.dk", port=5435)
     
    # Get GM data
    print('Getting GM data')
    quory = 'SELECT "TS_or_Distance","T","lat","lon","message" FROM "Measurements"  WHERE ("FK_Trip"=\'{0}\' AND "T"= \'acc.xyz\') ORDER BY "TS_or_Distance" ASC'.format(GM_trip)
    cursor = conn.cursor()
    sql_data = pd.read_sql(quory, conn, coerce_float = True)
  
    # Prepare data
    sql_data['GM_Acceleration_z'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.z'])
    sql_data['GM_Acceleration_x'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.x'])
    sql_data['GM_Acceleration_y'] = sql_data.message.apply(lambda message: loads(message)['acc.xyz.y'])  
    sql_data['GM_Acceleration_full'] = np.sqrt(sql_data.GM_Acceleration_x.values**2 + sql_data.GM_Acceleration_y.values**2 + sql_data.GM_Acceleration_z.values**2)
    sql_data.drop(columns=['T','message'],inplace=True,axis=1)
    
    for col in sql_data.columns:
        new_col = col if col.startswith('GM_')==True else 'GM_'+col
        sql_data.rename(columns={col:new_col},inplace=True)
        
    # Select nicely interpolated only
    if GM_trip == 'af1767f2-7bbb-4fbd-80bd-2cbe61b8c412':
        print('filtering')
        sql_data = sql_data[4200:]
    elif GM_trip == '9d056547-7872-4ae4-8fbe-3c61214c4c00':
        print('filtering')
        sql_data = sql_data[12000:20000]
    sql_data.reset_index(inplace=True, drop=True)
    
    # Get trip info
    print('Executing')
    quory = 'SELECT * FROM "Trips" WHERE "TripId"=\'{0}\''.format(GM_trip)
    cursor = conn.cursor()
    trips = pd.read_sql(quory, conn) 
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
    
    return sql_data, trips

def load_p79_data(FK_trip, GM_data):
    
    lat_max = GM_data.GM_lat.max()
    lat_min = GM_data.GM_lat.min()
    
    # Set up connection
    print("\nConnecting to PostgreSQL database to load p79")
    conn = psycopg2.connect(database="postgres", user="mibaj", password="mibajLira123", host="liradb.compute.dtu.dk", port=5435)
     
    # Execute quory: get sensor data
    print('Executing')
    quory = 'SELECT "DRDMeasurementId","TS_or_Distance","T","lat","lon","message" FROM "DRDMeasurements" WHERE ("FK_Trip"=\'{0}\' AND "lat"<={1} AND "lat">={2}) ORDER BY "TS_or_Distance" ASC'.format(FK_trip, lat_max, lat_min)
    #quory = 'SELECT * FROM "DRDMeasurements" WHERE ("FK_Trip"=\'{0}\' AND "lat"<={1} AND "lat">={2}) ORDER BY "TS_or_Distance" ASC'.format(FK_trip, lat_max, lat_min)
    cursor = conn.cursor()
    sql_data = pd.read_sql(quory, conn, coerce_float = True)

    # Comma to dot, then to float, then sort also in pandas after conversion
    sql_data.TS_or_Distance = sql_data.TS_or_Distance.map(lambda row: float(row.replace(',','.')))
    sql_data.sort_values(by ='TS_or_Distance', inplace=True)
    
    raw = sql_data[sql_data['T']=='raw data']
    prof = sql_data[sql_data['T']=='Profilometer']
    
    sql_data = raw.merge(prof,on=['TS_or_Distance','lat','lon'], suffixes=('_row','_prof'))
    sql_data.drop(columns=['T_prof','T_row','DRDMeasurementId_row','DRDMeasurementId_prof'],inplace=True,axis=1)
    sql_data.reset_index(inplace=True, drop=True)
    
    sql_data['DRD_Velocity'] = sql_data.message_row.apply(lambda message: loads(message)['Hastighed'])
    sql_data['DRD_Acceleration'] = sql_data.message_row.apply(lambda message: loads(message)['Acceleration'])
    sql_data['DRD_Laser5'] = sql_data.message_prof.apply(lambda message: loads(message)['Laser5'])
    sql_data['DRD_Laser21'] = sql_data.message_prof.apply(lambda message: loads(message)['Laser21'])  
    sql_data.drop(columns=['message_prof','message_row'],inplace=True,axis=1)
    
    for col in sql_data.columns:
        new_col = col if col.startswith('DRD_')==True else 'DRD_'+col
        sql_data.rename(columns={col:new_col},inplace=True)
   
     # Execute quory: get trip info
    print('Executing')
    #quory = 'SELECT * FROM "Trips" WHERE "TripId"=\'{0}\''.format(FK_trip)
    quory = 'SELECT * FROM "Trips"'
    cursor = conn.cursor()
    trips = pd.read_sql(quory, conn) 
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
    
    return sql_data, trips

def latlon_to_cartesian(lats,lons):
    R = 6371000 #m
    lats = np.deg2rad(lats)
    lons = np.deg2rad(lons)
    x = R * np.cos(lats) * np.cos(lons)
    y = R * np.cos(lats) * np.sin(lons)
    z = R *np.sin(lats)
    return x,y,z

def do_kNN_matching(GM_data, p79_data):
       
    #KNN
    p79_x, p79_y, p79_z = latlon_to_cartesian(p79_data.DRD_lat.values,  p79_data.DRD_lon.values)
    GM_x, GM_y, GM_z  = latlon_to_cartesian(GM_data.GM_lat.values,  GM_data.GM_lon.values)
    latlon = np.vstack([p79_x, p79_y, p79_z ]).T
    #latlon = np.vstack([GM_data.GM_lat.values,  GM_data.GM_lon.values]).T
    findNearestRegion = KDTree(latlon).query
    
    # Find closest neigbour
    dist_idx = [findNearestRegion(coords) for coords in zip(GM_x, GM_y, GM_z)] # dist_idx: (distance, p79_index)
    #dist_idx = [findNearestRegion(coords) for coords in zip(p79_data.DRD_lat.values, p79_data.DRD_lat.values)] 
    summed_dist = sum([i[0] for i in dist_idx])
    print(summed_dist) 
    #https://stackoverflow.com/questions/1185408/converting-from-longitude-latitude-to-cartesian-coordinates
        
    # Add columns to store matched p79 data in GM
    for col in p79_data.columns:
        GM_data[col] = np.nan
        GM_data[col] = GM_data[col].astype(p79_data.dtypes[col])
    GM_data['Distance'] = np.nan
    
    # Store matching results
    for GM_index, (distance, p79_index) in enumerate(dist_idx):
    #for p79_index, (distance, GM_index) in enumerate(dist_idx):  
        distance=distance
        if distance>0.5:
            continue
        print(p79_index, GM_index, distance)
        
        # p79 Match 
        p79_match = p79_data.iloc[p79_index]
        
        # Append data from the matched p79 to GM
        for col_name in  p79_data.columns:
            GM_data.at[GM_index,col_name] = p79_match[col_name]
        GM_data.at[GM_index,'Distance'] = distance
   
    GM_data.dropna(inplace=True)
    GM_data.drop('GM_TS_or_Distance',axis=1,inplace=True)
    GM_data.sort_values(by ='DRD_TS_or_Distance', inplace=True)
    GM_data.reset_index(inplace=True, drop=True)
    return dist_idx
    
def make_plots(data):

    from scipy import signal

    plt.figure()
    plt.title('p79: Acc, vel. and profile')
    plt.scatter(GM_data.DRD_TS_or_Distance, data.DRD_Velocity,s=1, c ='g',  alpha=0.5,label = 'p79 Velocity')
    plt.scatter(data.DRD_TS_or_Distance, 10*data.DRD_Acceleration, c='r',s=1, alpha=0.5, label= 'p79 Acceleration * 10')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Laser5,s=1,  alpha=0.5,label='Laser5')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Laser21,s=1, alpha=0.5, label='Laser21')
    plt.legend()
    plt.show()
    
    plt.figure()
    plt.title('p79 vs GM Accelerations')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Acceleration,c='r',s=4, alpha=0.5, label='p79 Acc')
    plt.scatter(data.DRD_TS_or_Distance, data['GM_Acceleration_full'],s=4, c='b', alpha=0.5, label='GM Acc_full')
    cor_full = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_full,mode='same')
    #plt.scatter(data.DRD_TS_or_Distance, cor_full, s=1, label='corr')
    plt.legend()
    plt.show()
    
    plt.figure()
    plt.title('p79 vs GM Accelerations')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Acceleration,c='r',s=4, alpha=0.5, label='p79 Acc')
    plt.scatter(data.DRD_TS_or_Distance, data.GM_Acceleration_x, c='b',s=4, alpha=0.5, label='GM Acc_x')
    cor_x = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_x,mode='same')
    #plt.scatter(data.DRD_TS_or_Distance, cor_x, s=1, label='corr')
    plt.legend()
    plt.show()
    
    plt.figure()
    plt.title('p79 vs GM Accelerations')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Acceleration,c='r',s=4, alpha=0.5, label='p79 Acc')
    cor_y = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_y,mode='same')
    plt.scatter(data.DRD_TS_or_Distance, data.GM_Acceleration_y, c='b',s=4, alpha=0.5,label='GM Acc_y')
    #plt.scatter(data.DRD_TS_or_Distance, cor_y, s=1, label='corr')
    plt.legend()
    plt.show()
    
    plt.figure()
    plt.title('p79 vs GM Accelerations')
    plt.scatter(data.DRD_TS_or_Distance, data.DRD_Acceleration,c='r',s=4, alpha=0.5, label='p79 Acc')
    plt.scatter(data.DRD_TS_or_Distance, data.GM_Acceleration_z, s=4,c='b',  alpha=0.5,label='GM Acc_z')
    cor_z = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_z,mode='same')
    #plt.scatter(data.DRD_TS_or_Distance, cor_z, s=1, label='corr')
    plt.legend()
    plt.show()

    # To excell
    data.to_excel(r'~/GM_p79.xlsx', index = False)

def plot_geolocation(longitudes, latitudes):
    import mplleaflet
    fig,ax=plt.subplots()
    ax.scatter(longitudes, latitudes, s = 12, c='b',marker='v') 
    mplleaflet.show()
    return

def plot_geolocation_2(gps_points_1, gps_points_2):
    import mplleaflet
    (lon1,lat1) = gps_points_1
    (lon2,lat2) = gps_points_2
    fig,ax=plt.subplots()
    ax.scatter(lon1, lat1, s = 20, c='red',marker='v',alpha=0.4)
    ax.scatter(lon2, lat2, s = 35, c='blue',marker='o',alpha=0.4) 
    mplleaflet.show()
    return
    
if __name__=='__main__':
    
    # === SETUP === #
    # ================#
    p79_trip = 'c6083ec8-d2a7-413c-8a92-795640a1dbfe'
    GM_trip = '9d056547-7872-4ae4-8fbe-3c61214c4c00' #4975
    gps_points_to_plot = -1# gps points to plot on a map
    
    # p79 trips
    # '825cfe3b-20f3-465f-ad4d-78dfe83c6112' H: towards north
    # '8b5b28b0-9034-4ee4-9cd2-6cc636a2b328' V: towards south, start at Hillerod
    # 'c6083ec8-d2a7-413c-8a92-795640a1dbfe', V:towards south, start at
    
    # GM relevant ones: 1666, 1672, 1673, 1685 
    #GM_trip = 'af1767f2-7bbb-4fbd-80bd-2cbe61b8c412' #1672
    #GM_trip = '2dcaefde-32a8-43e1-b005-25725ae9b234' #1666
    #GM_trip = 'fbe12e6d-4a79-44ca-aa73-30e5b65c1e3e' #1603
    
    
    # === GM DATA === #
    # ================#
    gps_points_to_plot=-1
    #GM_obd = load_obd_GM()
    GM_data, GM_trips = load_GM_data(GM_trip)    
    #plot_geolocation(GM_data.lon.values[0:gps_points_to_plot], GM_data.lat.values[0:gps_points_to_plot])

    # === p79 DATA === #
    # ================ #
    # Set up connection
    print("\nConnecting to PostgreSQL database to look for the closest p79 trip")
    conn = psycopg2.connect(database="postgres", user="mibaj", password="mibajLira123", host="liradb.compute.dtu.dk", port=5435)

    # Find p79 trips
    quory = 'SELECT DISTINCT "FK_Trip" FROM "DRDMeasurements"'
    cursor = conn.cursor()
    trips = pd.read_sql(quory, conn) 
    quory = 'SELECT * FROM "Trips"'
    cursor = conn.cursor()
    all_p79_trips = pd.read_sql(quory, conn) 
    
    # Close connection
    if(conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
    
    p79_data, p79_trips = load_p79_data(p79_trip, GM_data)

    #plot_geolocation_2((GM_data.GM_lon.values[0:gps_points_to_plot], GM_data.GM_lat.values[0:gps_points_to_plot]),(p79_data.DRD_lon.values[0:gps_points_to_plot], p79_data.DRD_lat.values[0:gps_points_to_plot]))
    
    # Filter GM part which is inside p79 lat/lon range
    GM_data = GM_data[GM_data.GM_lat.between(p79_data.DRD_lat.min(), p79_data.DRD_lat.max())]
    GM_data = GM_data[GM_data.GM_lon.between(p79_data.DRD_lon.min(), p79_data.DRD_lon.max())]
    GM_data.reset_index(inplace=True, drop=True)
    print('\n initial GM: ',GM_data.shape)
    #plot_geolocation(GM_data.lon.values[0:gps_points_to_plot], GM_data.lat.values[0:gps_points_to_plot])
    plot_geolocation_2((GM_data.GM_lon.values[0:gps_points_to_plot], GM_data.GM_lat.values[0:gps_points_to_plot]),
                       (p79_data.DRD_lon.values[0:gps_points_to_plot], p79_data.DRD_lat.values[0:gps_points_to_plot]))
    
    # === kNN Matching === #
    # ==================== #
    dist_idx = do_kNN_matching(GM_data, p79_data)

    plot_geolocation_2((GM_data.GM_lon.values[0:gps_points_to_plot], GM_data.GM_lat.values[0:gps_points_to_plot]),
                       (GM_data.DRD_lon.values[0:gps_points_to_plot], GM_data.DRD_lat.values[0:gps_points_to_plot]))
    #plot_geolocation( GM_data.DRD_lon.values[0:gps_points_to_plot], GM_data.DRD_lat.values[0:gps_points_to_plot] )


    # ===== Plots ======== #
    # ==================== #
    make_plots(GM_data)
    
    # Check correlations
    from scipy import signal
    cor_x = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_x, mode='same')
    cor_y = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_y,mode='same')
    cor_z = signal.correlate(GM_data.DRD_Acceleration, GM_data.GM_Acceleration_z,mode='same')

    print('mean corx: ',cor_x.mean())
    print('mean cory: ',cor_y.mean()) 
    print('mean corz: ',cor_z.mean())
    #from dtw import dtw,accelerated_dtw