#BASELINE PROCESSING

# To import specified link time series from SWMM output files


import os
import pandas as pd

#pyswmm is a popular python wrapper used for interacting with SWMM files
import pyswmm
from swmm.toolkit.shared_enum import NodeAttribute
from pyswmm import Output #Initialize the Output object to interact with the *.out file


#Get reference information
reference_info_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TY_ModelInformation.xlsx"
models_folder = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\06162023\InputModel_TY"

fwi_nodes = pd.read_excel(reference_info_location, sheet_name= "FWI_Nodes")
events = pd.read_excel(reference_info_location, sheet_name= "Events")
cso_outfalls =  pd.read_excel(reference_info_location, sheet_name = "Outfall_Nodes")

cso_outfalls = cso_outfalls.dropna()
cso_outfalls_list = cso_outfalls["Name"].astype(str).tolist()
fwi_nodes_list = fwi_nodes["Name"].tolist()

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
                save_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data_Baseline_5min"
                ts_df.to_csv(save_location + "/TS_CSO_" + folder + "_5min.csv")
                #Store FWI Heads
                ts_df=pd.DataFrame() #create an empty dataframe for working with each new output file
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
                save_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data_Baseline_5min"
                ts_df.to_csv(save_location + "/TS_FWI_Head_" + folder + "_5min.csv")                
    
#-----------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------

# To import specified link time series from SWMM output files

#Get reference information
reference_info_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TY_ModelInformation.xlsx"
raw_data_folder = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data_Baseline"
save_folder = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\Processed_Results"

fwi_nodes = pd.read_excel(reference_info_location, sheet_name= "FWI_Nodes")
events = pd.read_excel(reference_info_location, sheet_name= "Events", skiprows= 2)
cso_outfalls =  pd.read_excel(reference_info_location, sheet_name = "Outfall_Nodes")
cso_outfalls = cso_outfalls.dropna()
fwi_nodes_list = fwi_nodes["Name"].tolist()
cso_outfalls_nodename_list = cso_outfalls['Name'].astype(str).values.tolist()
cso_outfalls_csoname_list = cso_outfalls['CSO'].values.tolist()

#Get data
cso_data_all = pd.DataFrame()
fwi_data_all = pd.DataFrame()

for root, dirs,files in os.walk(raw_data_folder):
    for name in files: #loops through each file
        if "CSO" in name: #checks if CSO results
            resultpath = root+'\\'+name #assigns the file path of the output file to a variable
            cso_data = pd.read_csv(resultpath)
            cso_data['datetime'] = pd.to_datetime(cso_data['datetime'])
            cso_data = pd.merge_asof(cso_data, events[["Event", "StartDate", "EndDate"]], left_on= "datetime", right_on = "StartDate")
            cso_data = cso_data[(cso_data["datetime"] >= cso_data["StartDate"]) & (cso_data["datetime"] <= cso_data["EndDate"])]     
            cso_data = cso_data.drop(["StartDate", "EndDate", "Unnamed: 0", "datetime"], axis = 1)
            cso_data = cso_data.groupby(["Event", "scenario"]).sum().multiply(15*60*7.481/(10**6))
            cso_data_all = pd.concat([cso_data_all, cso_data])
        if "FWI" in name:
            resultpath = root+'\\'+name #assigns the file path of the output file to a variable
            fwi_data = pd.read_csv(resultpath)
            fwi_data['datetime'] = pd.to_datetime(fwi_data['datetime'])
            fwi_data = pd.merge_asof(fwi_data, events[["Event", "StartDate", "EndDate"]], left_on= "datetime", right_on = "StartDate")
            fwi_data = fwi_data[(fwi_data["datetime"] >= fwi_data["StartDate"]) & (fwi_data["datetime"] <= fwi_data["EndDate"])]
            fwi_data = fwi_data.drop(["StartDate", "EndDate", "Unnamed: 0", "datetime"], axis = 1)
            fwi_data = fwi_data.groupby(["Event", "scenario"]).max()
            fwi_data_all = pd.concat([fwi_data_all, fwi_data])
            
# Calculate relative surcharge and surcharge above rim

for index, row in fwi_nodes.iterrows():
    new_col = row['Name'] + '_rel'
    fwi_data_all[new_col] = ((fwi_data_all[row['Name']]- row['crown'])/ (row['rim'] - row['crown'])).round(2)
    new_col = row['Name'] + '_abvrim'
    fwi_data_all[new_col] = ((fwi_data_all[row['Name']]- row['rim'])*(fwi_data_all[row['Name']] > row['rim'])).round(2)

fwi_data_all = fwi_data_all.drop(fwi_nodes_list, axis = 1)

fwi_nodes_list = [x + "_rel" for x in fwi_nodes_list]
fwi_data_all['surcharge_count'] = (fwi_data_all[fwi_nodes_list]>=1).sum(axis = 1)
#--------------------------------------------------------------------------------------------------------------------

#replace CSO header column

cso_col_names = {cso_outfalls_nodename_list[i]: cso_outfalls_csoname_list[i] for i in range(len(cso_outfalls))}
cso_data_all = cso_data_all.rename(columns = cso_col_names)

#------------------------------------------------------

#Calculate total CSO volumes

cso_data_all['CSO_Total'] = cso_data_all[cso_outfalls['CSO'].values].sum(axis = 1)

#------------------------------------------------------

#Calculate CSO reduction

cso_data_all = cso_data_all.reset_index()
cso_data_all = cso_data_all.drop('scenario', axis = 1)
cso_col_names = {cso_data_all.columns[i]: cso_data_all.columns[i] + "_BL" for i in range(len(cso_data_all.columns))}
cso_data_all = cso_data_all.rename(columns = cso_col_names)

#------------------------------------------------------

#reset fwi data
fwi_data_all = fwi_data_all.reset_index()


#-----------------------------------------------------------


#join CSO Vol and surcharge results
cso_data_all["surcharge_count_BL"] = fwi_data_all["surcharge_count"]


#re-arrange dataframe
extract = cso_data_all[["Event_BL","CSO_Total_BL","surcharge_count_BL"]]
cso_data_all = cso_data_all.drop(["Event_BL","CSO_Total_BL","surcharge_count_BL"], axis = 1)
cso_data_all = pd.concat([extract, cso_data_all], axis = 1)

#print results
cso_data_all.to_csv(save_folder + "/CSO_Data_Baseline.csv", index = False)
fwi_data_all.to_csv(save_folder + "/FWI_Data_All_Baseline.csv", index = False)


































