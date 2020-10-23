"""
@author: Milena Bajic (DTU Compute)
"""
import pandas as pd
import numpy as np
from scipy.spatial import KDTree
import matplotlib.pyplot as plt

def latlon_to_cartesian(lats,lons):
    R = 6371000 # radium of the Earth in meters
    lats = np.deg2rad(lats)
    lons = np.deg2rad(lons)
    x = R * np.cos(lats) * np.cos(lons)
    y = R * np.cos(lats) * np.sin(lons)
    z = R *np.sin(lats)
    return x,y,z

def do_kNN_matching(GM_data, DRD_data, max_distance = 1):
    # GM data will be equal to the final matched data
    DRD_x, DRD_y, DRD_z = latlon_to_cartesian(DRD_data.DRD_lat.values,  DRD_data.DRD_lon.values)
    GM_x, GM_y, GM_z  = latlon_to_cartesian(GM_data.GM_lat.values,  GM_data.GM_lon.values)
    latlon = np.vstack([DRD_x, DRD_y, DRD_z ]).T
    kd_tree = KDTree(latlon)
    findNearestRegion = kd_tree.query
    
    # Find the closest neigbour (the one with the minimum eucledian distance)
    dist_idx = [findNearestRegion(coords) for coords in zip(GM_x, GM_y, GM_z)] # dist_idx: (distance, DRD_index)
        
    # Set dataframe to store matches
    matched_data = GM_data.copy()
    for col in DRD_data.columns:
        matched_data[col] = np.nan
        matched_data[col] = matched_data[col].astype(DRD_data.dtypes[col])
    matched_data['Distance'] = np.nan
    
    # Filter on maximum distance and save matching results
    for GM_index, (distance, DRD_index) in enumerate(dist_idx):
        
        # Save only matched with distance smaller than this
        if distance > max_distance:
            continue
    
        # DRD match
        DRD_match = DRD_data.iloc[DRD_index]
        
        # Append data from the matched DRD to GM
        for col_name in  DRD_data.columns:
            matched_data.at[GM_index,col_name] = DRD_match[col_name]
        matched_data.at[GM_index,'Distance'] = distance
        #print(DRD_index, GM_index, distance)
   
    # Drop non-matched
    matched_data.dropna(inplace=True)
    matched_data.drop('GM_TS_or_Distance',axis=1,inplace=True)
    matched_data.sort_values(by ='DRD_TS_or_Distance', inplace=True)
    matched_data.reset_index(inplace=True, drop=True)
    
    # Save matched result as excel file
    matched_data.to_excel(r'~/matched_output.xlsx', index = False)
    
    return matched_data, dist_idx
 

