# To import specified link time series from SWMM output files

import os
import pandas as pd

#pyswmm is a popular python wrapper used for interacting with SWMM files
import pyswmm
from swmm.toolkit.shared_enum import NodeAttribute #this line is to work with Node data
from pyswmm import Output #Initialize the Output object to interact with the *.out file


ts_df_all = pd.DataFrame() #Initialize data frame

#open file with elements and store in list
txt_file = open(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Interim\2022-0824 INTERIM_LMRDP_IC.A.5\LMRDP_CSO_Level_MH_List.txt")
file_content = txt_file.read()
node_id = file_content.split("\n") #split the contents by new line
txt_file.close()


#looks through all files within directory and finds output files
for root, dirs,files in os.walk(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\2022-1115 DCST Pre-Screening\189"):
    for name in files: #loops through each file
        ext = name.rpartition('.')[2] #gets the extension of the file
        if ext in ("out","OUT"): #checks if file is an output file
            folder = root.rpartition('\\')[2] #gets the name of the folder in which the file exists
            resultpath = root+'\\'+name #assigns the file path of the output file to a variable
            with Output(resultpath) as out: #need this line for reading from *.out file
                ts_df=pd.DataFrame() #create an empty dataframe for working with each new output file
                for x in node_id: #cycles through all the elements in the list
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.node_series(x, NodeAttribute.INVERT_DEPTH) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3) #round to 3 decimal places
                    else: #when dataframe is already populated
                        ts_temp = out.node_series(x, NodeAttribute.INVERT_DEPTH) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df = pd.merge(ts_df, ts_df_temp ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                        ts_df[x] = ts_df[x].round(3) #round to 3 decimal places
                ts_df = ts_df.assign(scenario = folder) #add column called scenario whose value is the variable folder
                ts_df_all = pd.concat([ts_df_all, ts_df]) #append the ts_df time series to the ts_df_all time series.
                
filename = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Interim\2022-0824 INTERIM_LMRDP_IC.A.5\LMRDP_CSO_Level_MH_TS_Output.txt"
ts_df_all.to_csv(filename, sep='\t', index = False)



