
# kNN_track_matching
Module for finding matching GPS points of two trajectories, using k-Mearest Neighbor algorithm.  
(author: Milena Bajic) <br/>

Instructions:<br/>
1. Install packages: <br/>
   pandas, numpy, matplotlib <br/>
   psycopg2 to connect to the database: pip install psycopg2==2.7.7 (or conda if using conda) <br/>
   mplleaflet to plot locations on OSM:  pip install mplleaflet (or conda install -c conda-forge mplleaflet)<br/>
2. Run do_matching.py with chosen parameters

Example output of the matching result (matched GPS points of different trajectories are shown in red and blue):
![Example output with matched trajectories](example.png)
