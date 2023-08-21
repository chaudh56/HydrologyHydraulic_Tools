# To import specified link time series from SWMM output files

import os
import pandas as pd

#pyswmm is a popular python wrapper used for interacting with SWMM files
import pyswmm
from swmm.toolkit.shared_enum import LinkAttribute
from pyswmm import Output #Initialize the Output object to interact with the *.out file


ts_df_all = pd.DataFrame() #Initialize data frame

#open file with elements and store in list
txt_file = open(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Analysis\RegionalControl\Dashboard\Reference Text Files\LMRDP_Gate_Setting_LinkList.txt")
file_content = txt_file.read()
link_id = file_content.split("\n")
txt_file.close()


#looks through all files within directory and finds output files
for root, dirs,files in os.walk(r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Interim\2022-0824 INTERIM_LMRDP_IC.A.5\10yr Event Selection\188"):
    for name in files: #loops through each file
        ext = name.rpartition('.')[2] #gets the extension of the file
        if ext in ("out","OUT"): #checks if file is an output file
            folder = root.rpartition('\\')[2] #gets the name of the folder in which the file exists
            resultpath = root+'\\'+name #assigns the file path of the output file to a variable
            with Output(resultpath) as out: #need this line for reading from *.out file
                ts_df=pd.DataFrame() #create an empty dataframe for working with each new output file
                for x in link_id: #cycles through all the elements in the list
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.link_series(x, LinkAttribute.CAPACITY) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3)
                    else: #when dataframe is already populated
                        ts_temp = out.link_series(x, LinkAttribute.CAPACITY) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df = pd.merge(ts_df, ts_df_temp ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                        ts_df[x] = ts_df[x].round(3)
                ts_df = ts_df.assign(scenario = folder)
                ts_df_all = pd.concat([ts_df_all, ts_df])
                
filename = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Analysis\RegionalControl\Dashboard\Reference Text Files\LMRDP_Gate_Setting_TS_Output.txt"
ts_df_all.to_csv(filename, sep='\t', index = False)



