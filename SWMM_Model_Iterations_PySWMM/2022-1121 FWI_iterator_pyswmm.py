# -*- coding: utf-8 -*-
"""
Created on Sun Nov 13 17:52:48 2022

@author: PEREIRJ1

This is a test script to trial iterating over model runs with pyswmm
Here, chose the downstream CSO L-008 for testing with scenario 1058. It is a small CSO and small event.
Before starting the simulation, need to remove control rules for the orifice as the control rules will be set by the script instead
"""



from pyswmm import Simulation, Nodes, Links 
import pandas as pd
import datetime as dt


orifice_temp_setting = 0.25 #baseline orifice setting during wet weather
node1_inflow = 1 #initialize node inflow value to be greater than 0
start_time = dt.datetime(2011,7,12,17,25,0) #identified starting timestep of CSO at L-008 from earlier model run to minimize script runtime
end_time = dt.datetime(2011,7,12,18,10,0) #identified ending timestep of CSO at L-008 from earlier model run to minimize script runtime to minimize script runtime

# Loop over simulations by incrementally increasing orifice setting to identify max. possible setting that maximizes FWI usage and minimizes CSO. 
#Exits loop if CSO is small (< 0.001 MG) or max. possible orifice setting is reached (1)
while node1_inflow > 0.001 and orifice_temp_setting <= 1:
    with Simulation(r"C:\Users\PereirJ1\OneDrive - Jacobs\Desktop\Trial Iterator\IC.A.5_INTERIM_LMRDP_08_17_1058.inp") as sim:
        node1 = Nodes(sim)["26G4-216D"] #tracking L-008 outfall
        sensor_node = Nodes(sim)["26G4-077C"] #tracking depth at L-008 sensor node
        target_orifice = Links(sim)["I-002i1"] #setting orifice setting
        result_node = Nodes(sim)["Diversion"] #tracking depth at the downstream-most storage node on the FWI called "Diversion"

        # Initializing some empty dataframes to store values during simulation
        idx = []
        node1_values = []
        sensor_node_depth = []
        target_orifice_setting_values = []
        
      # for timesteps during which CSO was occuring, apply the current orifice control rule and track variables
        for step in sim:
            if start_time <= sim.current_time <= end_time:
                if sensor_node.depth >= 1.7:
                    target_orifice.target_setting = orifice_temp_setting
                idx.append(sim.current_time)
                node1_values.append(node1.cumulative_inflow/10**6)
                sensor_node_depth.append(sensor_node.depth)
                target_orifice_setting_values.append(target_orifice.current_setting)
                node1_inflow = node1.cumulative_inflow/10**6
                result_node_value = result_node.depth
   
    # At end of each simulation, printing final results
    print("\nFor setting " + str(orifice_temp_setting) + ", CSO is " + str(node1_inflow) + " MG and Diversion Depth is " + str(result_node_value) + " ft")
    
    #incrementing orifice setting
    if orifice_temp_setting == 0.25:
        orifice_temp_setting += 0.05
    else:
        orifice_temp_setting += 0.1
        
    #Storing recorded time series in a dataframe
    df = pd.DataFrame({'node1_cum_inflow':node1_values, 'node_depth': sensor_node_depth, 'orifice_setting': target_orifice_setting_values}, index = idx)

