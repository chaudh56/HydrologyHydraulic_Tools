# To import specified link time series from SWMM output files

import os
import pandas as pd
import numpy as np

#Get reference information
reference_info_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TY_ModelInformation.xlsx"
raw_data_folder = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\TS_Data"
save_folder = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\Processed_Results"
baseline_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\Processed_Results\CSO_Data_Baseline.csv"
baseline_fwi_location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\TY_Results_Analysis\Processed_Results\FWI_Data_All_Baseline.csv"

fwi_nodes = pd.read_excel(reference_info_location, sheet_name= "FWI_Nodes")
events = pd.read_excel(reference_info_location, sheet_name= "Events", skiprows= 2)
cso_outfalls =  pd.read_excel(reference_info_location, sheet_name = "Outfall_Nodes")
cso_outfalls = cso_outfalls.dropna()
fwi_nodes_list = fwi_nodes["Name"].tolist()
cso_outfalls_nodename_list = cso_outfalls['Name'].astype(str).values.tolist()
cso_outfalls_csoname_list = cso_outfalls['CSO'].values.tolist()
baseline_data = pd.read_csv(baseline_location)
baseline_fwi_data = pd.read_csv(baseline_fwi_location)

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

# Calculate relative surcharge

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

#Calculate CSO reduction relative to baseline

cso_data_all = cso_data_all.reset_index()

cso_data_all = pd.merge(cso_data_all, baseline_data.drop("surcharge_count_BL", axis = 1), left_on = "Event", right_on = "Event_BL")
cso_data_all[cso_outfalls['CSO'].values + "_red"] = cso_data_all[cso_outfalls['CSO'].values + "_BL"] - cso_data_all[cso_outfalls['CSO'].values].values
cso_data_all["CSO_Total_Red"] = cso_data_all["CSO_Total_BL"] -  cso_data_all["CSO_Total"] 
cso_data_all = cso_data_all.drop(list(cso_data_all.filter(regex = "_BL")), axis = 1)


#------------------------------------------------------

#Calculate surcharge count relative to baseline
fwi_data_all = fwi_data_all.reset_index()
fwi_data_all_2 = fwi_data_all

fwi_data_all = pd.merge(fwi_data_all, baseline_data[["Event_BL","surcharge_count_BL"]], left_on = "Event", right_on = "Event_BL" )
fwi_data_all["surcharge_count_red"] = fwi_data_all["surcharge_count_BL"] - fwi_data_all["surcharge_count"]
fwi_data_all = fwi_data_all.drop(["surcharge_count_BL", "Event_BL"], axis = 1)


#count locations with surcharge > baseline and exceeding rim
fwi_col_names = {baseline_fwi_data.columns[i]: baseline_fwi_data.columns[i] + "_BL" for i in range(len(baseline_fwi_data.columns))}
baseline_fwi_data = baseline_fwi_data.rename(columns = fwi_col_names)

fwi_data_all = pd.merge(fwi_data_all, baseline_fwi_data, left_on = "Event", right_on = "Event_BL")

scenario_col_names_rel = [k for k in list(fwi_col_names.keys()) if k not in ["surcharge_count", "Event", "scenario"] and k.endswith("_rel")]
scenario_col_names_abvrim = [k for k in list(fwi_col_names.keys()) if k not in ["surcharge_count", "Event", "scenario"] and k.endswith("_abvrim")]
bl_col_names_rel = [k for k in list(fwi_col_names.values()) if k not in ["surcharge_count_BL", "Event_BL", "scenario_BL"] and k.endswith("_rel_BL")]
bl_col_names_abvrim = [k for k in list(fwi_col_names.values()) if k not in ["surcharge_count_BL", "Event_BL", "scenario_BL"] and k.endswith("_abvrim_BL")]
fwi_data_all["surcharge_total_rel_exceedance"] = ((fwi_data_all[scenario_col_names_rel] > fwi_data_all[bl_col_names_rel].values)*(fwi_data_all[scenario_col_names_rel] >= 1)*(fwi_data_all[scenario_col_names_rel] - np.maximum(fwi_data_all[bl_col_names_rel].values,1))).sum(axis = 1)
fwi_data_all["surcharge_count_exceedance"] = ((fwi_data_all[scenario_col_names_rel] > fwi_data_all[bl_col_names_rel].values)*(fwi_data_all[scenario_col_names_rel] >= 1)).sum(axis = 1)

u = (fwi_data_all[scenario_col_names_abvrim] - fwi_data_all[bl_col_names_abvrim].values).values
v = (fwi_data_all[scenario_col_names_abvrim]).values
w = np.minimum(u,v)


fwi_data_all["surcharge_total_abvrim_exceedance"] = ((fwi_data_all[scenario_col_names_rel] > fwi_data_all[bl_col_names_rel].values)*(fwi_data_all[scenario_col_names_rel] >= 1)*w).sum(axis = 1)

fwi_exceedance = w*(fwi_data_all[scenario_col_names_rel] > fwi_data_all[bl_col_names_rel].values)*(fwi_data_all[scenario_col_names_rel] >= 1)
fwi_exceedance[["Event", "scenario"]] = fwi_data_all[["Event", "scenario"]]

fwi_data_all = fwi_data_all.drop(list(fwi_data_all.filter(regex = "_BL")), axis = 1)

#-----------------------------------------------------------


#join CSO Vol and surcharge results
cso_data_all[["surcharge_count", "surcharge_count_red", "surcharge_total_rel_exceedance", "surcharge_count_exceedance", "surcharge_total_abvrim_exceedance"]] = fwi_data_all[["surcharge_count", "surcharge_count_red", "surcharge_total_rel_exceedance", "surcharge_count_exceedance", "surcharge_total_abvrim_exceedance"]]


#add new scenario columns
cso_data_all["Run_Group"] = cso_data_all["scenario"].str.split('_').str[1]
cso_data_all["Scenario_ID"] = cso_data_all["scenario"].str.split('FullTY_').str[-1]



#re-arrange dataframe
extract = cso_data_all[["Event","scenario","Run_Group","Scenario_ID","CSO_Total","CSO_Total_Red","surcharge_count", "surcharge_count_red", "surcharge_total_rel_exceedance", "surcharge_count_exceedance", "surcharge_total_abvrim_exceedance"]]
cso_data_all = cso_data_all.drop(["Event", "scenario","Run_Group","Scenario_ID", "CSO_Total","CSO_Total_Red","surcharge_count", "surcharge_count_red", "surcharge_total_rel_exceedance", "surcharge_count_exceedance", "surcharge_total_abvrim_exceedance"], axis = 1)
cso_data_all = pd.concat([extract, cso_data_all], axis = 1)

#print results
cso_data_all.to_csv(save_folder + "/CSO_Data_IC9.csv", index = False)
fwi_data_all.to_csv(save_folder + "/FWI_Data_All_IC9.csv", index = False)
fwi_exceedance.to_csv(save_folder + "/FWI_Exceedance_IC9.csv", index = False)









