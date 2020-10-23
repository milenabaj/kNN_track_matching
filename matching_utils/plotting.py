"""
@author: Milena Bajic (DTU Compute)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplleaflet

def plot_geolocation(longitudes, latitudes):
    fig,ax=plt.subplots()
    ax.scatter(longitudes, latitudes, s = 12, c='b',marker='v') 
    mplleaflet.show()
    return

def plot_geolocation_2(gps_points_1, gps_points_2):
    (lon1,lat1) = gps_points_1
    (lon2,lat2) = gps_points_2
    
    fig,ax=plt.subplots()
    ax.scatter(lon1, lat1, s = 15, c='red',marker='v',alpha=0.4)
    ax.scatter(lon2, lat2, s = 20, c='blue',marker='o',alpha=0.4) 
    mplleaflet.show()
    return
 