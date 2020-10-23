"""
@author: Milena Bajic (DTU Compute)
"""
import os, sys, glob, pickle, argparse
import pandas as pd
import numpy as np
from matching_utils.data_loaders import *
from matching_utils.matching import *
from matching_utils.plotting import *  

if __name__=='__main__':
    
    # ====== SETUP ====== #
    # =================== #
    # Script arguments
    parser = argparse.ArgumentParser(description='Please provide command line arguments.')
    
    parser.add_argument('--GM_trip_id', default = '9d056547-7872-4ae4-8fbe-3c61214c4c00',
                        help='Unique string defining the GM trip.')
    parser.add_argument('--DRD_trip_id', default = 'c6083ec8-d2a7-413c-8a92-795640a1dbfe',
                        help='Unique string defining the DRD trip.')
    parser.add_argument('--max_distance', default = 5, type=int,
                        help = 'Maximum distance in meters between the 2 closest points to consider them a match.')
    parser.add_argument('--plot_n_gps_points', default = -1, type=int,
                        help = 'Number of points on the map. Choose -1 for all.')

    # Parse arguments
    args = parser.parse_args()
    GM_trip_id = args.GM_trip_id
    DRD_trip_id = args.DRD_trip_id
    max_distance = args.max_distance
    gps_points_to_plot = args.plot_n_gps_points
    # ================== #
    
    # GM data
    GM_data, GM_trip_info = load_GM_data(GM_trip_id)    
    plot_geolocation(GM_data.GM_lon.values[0:gps_points_to_plot], GM_data.GM_lat.values[0:gps_points_to_plot])

    # DRD data 
    DRD_data, DRD_trips = load_DRD_data(DRD_trip_id, lat_min = GM_data.GM_lat.min(), lat_max =  GM_data.GM_lat.max())
    plot_geolocation(DRD_data.DRD_lon.values[0:gps_points_to_plot], DRD_data.DRD_lat.values[0:gps_points_to_plot])
    
    # Filter GM data 
    GM_data = filter_latlon(GM_data,'GM', lat_min = DRD_data.DRD_lat.min(), lat_max = DRD_data.DRD_lat.max(), lon_min = DRD_data.DRD_lon.min(), lon_max = DRD_data.DRD_lon.max())

    # Match two trajectories using kNN
    matched_data, dist_idx = do_kNN_matching(GM_data, DRD_data, max_distance = max_distance)
    plot_geolocation_2((matched_data.GM_lon.values[0:gps_points_to_plot], matched_data.GM_lat.values[0:gps_points_to_plot]),
                       (matched_data.DRD_lon.values[0:gps_points_to_plot], matched_data.DRD_lat.values[0:gps_points_to_plot]))

