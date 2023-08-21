# To import specified link time series from SWMM output files

import os
import pandas as pd

#pyswmm is a popular python wrapper used for interacting with SWMM files
import pyswmm
from swmm.toolkit.shared_enum import NodeAttribute
from pyswmm import Output #Initialize the Output object to interact with the *.out file


#Get reference information
reference_info_location = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TY_ModelInformation.xlsx"
models_folder = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\06162023\Results\SWMM_35event_inputs\UU\FullTY\ReRun_07312023"

fwi_nodes = pd.read_excel(reference_info_location, sheet_name= "FWI_Nodes")
events = pd.read_excel(reference_info_location, sheet_name= "Events")
cso_outfalls =  pd.read_excel(reference_info_location, sheet_name = "Outfall_Nodes")

cso_outfalls = cso_outfalls.dropna()
cso_outfalls_list = cso_outfalls["Name"].astype(str).tolist()
fwi_nodes_list = fwi_nodes["Name"].tolist()
flooding_list = ["tunDCT01", "J3"]
surcharge_list = ["UCB33361", "LindP7"]

#Initialize data frames
#ts_cso_all = pd.DataFrame()
#ts_fwi_all = pd.DataFrame()


#looks through all files within directory and finds output files
for root, dirs,files in os.walk(models_folder):
    for name in files: #loops through each file
        ext = name.rpartition('.')[2] #gets the extension of the file
        if ext in ("out","OUT"): #checks if file is an output file
            folder = root.rpartition('\\')[2] #gets the name of the folder in which the file exists
            resultpath = root+'\\'+name #assigns the file path of the output file to a variable
            print(name)
            with Output(resultpath) as out: #need this line for reading from *.out file
                #Store CSO Inflows
                ts_df=pd.DataFrame() #create an empty dataframe for working with each new output file
                i = 0
                for x in cso_outfalls_list: #cycles through all the elements in the list
                    print(str(i) + "-" + x)
                    i = i+1
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.node_series(x, NodeAttribute.TOTAL_INFLOW) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3)
                    else: #when dataframe is already populated
                        ts_temp = out.node_series(x, NodeAttribute.TOTAL_INFLOW) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.values(), columns = [x]) #convert the dictionary to a dataframe
                        ts_df_temp[x] = ts_df_temp[x].round(3)
                        ts_df = pd.concat([ts_df, ts_df_temp], axis = 1 ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                ts_df = ts_df.assign(scenario = folder)
                save_location = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data"
                ts_df.to_csv(save_location + "/TS_CSO_" + folder + ".csv")
                #Store FWI Heads
                ts_df=pd.DataFrame() #create an empty dataframe for working with each new output file
                i = 0
                for x in surcharge_list: #cycles through all the elements in the list
                    print(str(i) + "-" + x)
                    i = i+1
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.node_series(x, NodeAttribute.HYDRAULIC_HEAD) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3)
                    else: #when dataframe is already populated
                        ts_temp = out.node_series(x, NodeAttribute.HYDRAULIC_HEAD) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.values(), columns = [x]) #convert the dictionary to a dataframe
                        ts_df_temp[x] = ts_df_temp[x].round(3)
                        ts_df = pd.concat([ts_df, ts_df_temp], axis = 1 ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                ts_df = ts_df.assign(scenario = folder)
                save_location = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data"
                ts_df.to_csv(save_location + "/TS_Sur_Head_" + folder + ".csv")
                i = 0
                for x in fwi_nodes_list: #cycles through all the elements in the list
                    print(str(i) + "-" + x)
                    i = i+1
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.node_series(x, NodeAttribute.HYDRAULIC_HEAD) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3)
                    else: #when dataframe is already populated
                        ts_temp = out.node_series(x, NodeAttribute.HYDRAULIC_HEAD) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.values(), columns = [x]) #convert the dictionary to a dataframe
                        ts_df_temp[x] = ts_df_temp[x].round(3)
                        ts_df = pd.concat([ts_df, ts_df_temp], axis = 1 ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                ts_df = ts_df.assign(scenario = folder)
                save_location = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data"
                ts_df.to_csv(save_location + "/TS_FWI_Head_" + folder + ".csv")                
                i = 0
                for x in flooding_list: #cycles through all the elements in the list
                    print(str(i) + "-" + x)
                    i = i+1
                    if ts_df.empty: #for first time data frame is populated
                        ts_temp = out.node_series(x, NodeAttribute.FLOODING_LOSSES) #data gets read as a dictionary
                        ts_df = pd.DataFrame(ts_temp.items(), columns = ["datetime", x]) #convert the dictionary to a dataframe
                        ts_df[x] = ts_df[x].round(3)
                    else: #when dataframe is already populated
                        ts_temp = out.node_series(x, NodeAttribute.FLOODING_LOSSES) #data gets read as a dictionary            
                        ts_df_temp = pd.DataFrame(ts_temp.values(), columns = [x]) #convert the dictionary to a dataframe
                        ts_df_temp[x] = ts_df_temp[x].round(3)
                        ts_df = pd.concat([ts_df, ts_df_temp], axis = 1 ) #joining the temp dataframe to the existing dataframe. Since no match columns are specified, function will join based on the common columns, here the common columns are "datetime"           
                ts_df = ts_df.assign(scenario = folder)
                save_location = r"C:\Users\md071288\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data"
                ts_df.to_csv(save_location + "/TS_Flood_" + folder + ".csv")     
