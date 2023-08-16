# To import specified link time series from SWMM output files

import os
import pandas as pd
from swmmio import Model, Nodes


result_df_all = pd.DataFrame() #Initialize data frame


node_id = ["GRA32061", "GRA31538", "GRA50116", "GRA29893","GRA28398","GRA60442"] #list of link elements for which we want time series


#looks through all files within directory and finds output files
for root, dirs,files in os.walk(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Interim\2022-0824 INTERIM_LMRDP_IC.A.5\10yr Event Selection\188"):
    for name in files: #loops through each file
        ext = name.rpartition('.')[2] #gets the extension of the file
        if ext in ("inp","INP"): #checks if file is an output file
            folder = root.rpartition('\\')[2] #gets the name of the folder in which the file exists
            m = Model(root + "\\" + name)
            nodes = Nodes(
                model=m,
                inp_sections=['junctions', 'storages', 'outfalls'],
                rpt_sections=['Node Surcharge Summary', 'Node Depth Summary'],
                columns=[ 'MinDepthBelowRim','MaxHeightAboveCrown', 'MaxNodeDepth'])
            result_df = nodes.dataframe.loc[node_id] # access data and subset to element list
            result_df = result_df.assign(scenario = folder) #adds scenario column based on folder        
            result_df_all = pd.concat([result_df_all, result_df]) #appends the collected time series for one scenario to the data frame for all scenarios

#final format and save
result_df_all.insert(0,'scenario', result_df_all.pop('scenario')) #re-arranges columns by "popping" out the scenario column and placing it at the first position (0) with the same heading name
result_df_all.to_csv(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Analysis\Storage Pre-Screen\trial.txt", sep='\t', index = True) #save file

root + "\\" + name
