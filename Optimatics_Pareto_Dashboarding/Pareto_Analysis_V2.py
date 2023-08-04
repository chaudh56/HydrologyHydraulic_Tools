# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import numpy as np
import pandas as pd
import os
#import matplotlib.pyplot as plt
# import plotly


#========== REQUIRED INPUTS
analysis_file_dir = {'T9_v2.1.0': ['Test_data_from_Dan/', 'OPTI_FMR_T9_v2.1.0_ConveyanceAndStore.xlsx'],
                     'T9_v2.0.2': ['./', 'OPTI_FMR_T9_v2.0.2_StoreOnly_AllLocations.xlsx'],
                     'T9_v2.0.1': ['./', 'OPTI_FMR_T9_v2.0.1_StoreOnly (1).xlsx'],
                     'T9_v2.0.0': ['./', 'OPTI_FMR_T9_v2.0.0_ConveyanceOnly (1).xlsx']}

global Objective_Column_Names
Objective_Column_Names = ['O: FloodPenalty', 'O: Cost'] #from left to right as in the excel sheet

#================================
def Run_Parteto_analysis(Project_Name, iDIR, Excel_file_name):
    Excel_file_name =Excel_file_name
    Project_Name =Project_Name
    iDIR =iDIR
    #Set Directory
    os.chdir(iDIR)

    #Read Input Data
    df_plans = pd.read_excel(Excel_file_name,'Plans')
    df_rows = pd.read_excel(Excel_file_name,'Species (Rows)')
    df_decision_schema = pd.read_excel(Excel_file_name,'Decision Schema')

    #Generate variables fof the process
    OCN=Objective_Column_Names
    Plan_names = df_plans['Plan name'].tolist()[df_plans['Plan name'].tolist().index('Encoding')+1:]
    Plan_names_Txt = [i +'_TXT' for i in Plan_names]

    #
    df_rows_1 = df_rows.copy()
    df_rows_1[Plan_names] = df_rows_1['Encoding'].str.split(',',expand=True)
    df_rows_1[Plan_names[0]]=df_rows_1[Plan_names[0]].str.replace('[', '', regex=True)
    df_rows_1[Plan_names[-1]]=df_rows_1[Plan_names[-1]].str.replace(']', '', regex=True)


    df_rows_2 = df_rows_1.copy()
    for pp in range(len(Plan_names_Txt)):
        res = df_decision_schema.loc[df_decision_schema['Decision'] == Plan_names[pp]]
        res = res.reset_index(drop=True)
        description_dict = dict({res['Encoding Value'][i]: res['Outcome Description'][i] for i in range(len(res['Encoding Value']))})
        df_rows_2[Plan_names_Txt[pp]] = [description_dict[int(i)] for i in df_rows_2[Plan_names[pp]]]
        

    
    #Calculate Pareto
    df_rows_3 = df_rows.copy()
    df_rows_3.insert(df_rows_3.columns.get_loc('Encoding'),'Pareto','')
    df_rows_3.insert(df_rows_3.columns.get_loc('Pareto'),'Generation','')

    aa = "df_rows_3['{}'] < df_rows_3['{}'][i]".format(Objective_Column_Names[1],Objective_Column_Names[1])
    bb = "df_rows_3['{}'] < df_rows_3['{}'][i]".format(Objective_Column_Names[0],Objective_Column_Names[0])
    df_rows_3['Pareto']=[len(df_rows_3.loc[eval(aa) & eval(bb)]) for i in range(len(df_rows_3))]

    df_rows_3['Generation']=np.array(df_rows_3.index.tolist())//1000*1


    #Remove Duplicates
    df_rows_4=df_rows_2.copy()
    df_rows_4.insert(df_rows_4.columns.get_loc('Encoding'),'Pareto','')
    df_rows_4.insert(df_rows_4.columns.get_loc('Encoding'),'Generation','')
    df_rows_4['Pareto']=df_rows_3['Pareto']
    df_rows_4['Generation']=df_rows_3['Generation']

    df_rows_4=df_rows_4.dropna()
    PC_columnanmes=df_rows_3.columns[:df_rows_3.columns.get_loc(OCN[0])]
    df_rows_4=df_rows_4.drop_duplicates(subset=PC_columnanmes,keep='first')
    # df_rows_4=df_rows_4.drop(df_rows_4[df_rows_4['Pareto'] >0].index)
    # df_rows_4=df_rows_4.sort_values(by=['O: DiversionPenalty'])

    
    df_rows_4.to_csv(Project_Name+'_Pareto_Analysis.csv')


for project in analysis_file_dir:
    print(project, analysis_file_dir[project][0], analysis_file_dir[project][1])
    Run_Parteto_analysis(project, analysis_file_dir[project][0], analysis_file_dir[project][1])





















