# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 14:05:08 2022

@author: CHAUDHS1
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import os
import numpy as np


# =============== REQUIRED INPUTS ===============================================

# Project Name
project_label="Alexandria Optimization"

# Location of the csv Filename
# Will only work with the post processed excel files from python script
analysis_file_dir = {'T9_v2.1.0': ['Test_data_from_Dan/','T9_v2.1.0_Pareto_Analysis.csv'],
                     'T9_v2.0.2': ['Test_data_from_Dan/', 'T9_v2.0.2_Pareto_Analysis.csv'],
                     'T9_v2.0.1': ['Test_data_from_Dan/', 'T9_v2.0.1_Pareto_Analysis.csv'],
                     'T9_v2.0.0': ['Test_data_from_Dan/', 'T9_v2.0.0_Pareto_Analysis.csv']}

# Name of the data column you want to be active for tracing on plot, should be common among all pareto datasheets
Variable_for_Trace='PC: DiversionCost'

for dataset in analysis_file_dir:
    if os.path.isdir(analysis_file_dir[dataset][0]):
        if os.path.isfile(os.path.join(analysis_file_dir[dataset][0],analysis_file_dir[dataset][1])):
            analysis_file_dir[dataset].append(os.path.join(analysis_file_dir[dataset][0],analysis_file_dir[dataset][1]))
        else:
            print('The file {} does not exist but directory {} was found'.format(analysis_file_dir[dataset][1],analysis_file_dir[dataset][0]))
            raise SystemExit(0)
    else:
        print('file directory {} was not find; check spelling and try again'.format(analysis_file_dir[dataset][0]))
        raise SystemExit(0)


# ===============================================================================

#%%
#OrigDir = os.getcwd()
my_paretos = {'UniqueDecisions': [], 'MaxGeneration':0, 'DecisionSchema': []}
for dataset in analysis_file_dir:
    print("loading pareto for {}".format(dataset))
    my_paretos[dataset]=pd.read_csv(analysis_file_dir[dataset][2], header = 0, encoding='utf-8', low_memory = False, on_bad_lines = 'warn')#, sheet_name='Species (Rows)')
    columns = my_paretos[dataset].columns
    
    my_paretos["Obj_columns" + dataset]=[col for col in columns if 'O:' in col]#['O: FloodPenalty','O: Cost Corrected']
    my_paretos["PC_columns" + dataset]=[col for col in columns if 'PC:' in col]
    my_paretos["Plan_columns" + dataset]= [col[:-4] for col in columns if '_TXT' in col]
    my_paretos['UniqueDecisions'].extend([col for col in columns if '_TXT' in col])
    my_paretos["MaxGeneration"] = max(my_paretos["MaxGeneration"], max(list(my_paretos[dataset]['Generation'])))
    print('loading complete')



#os.chdir(OrigDir)
#%%
colors = {"background": "#011833", "text": "#7FDBFF"}

app = dash.Dash()
app.layout = html.Div(
        [
        # Plot title
        html.H1(project_label, style={'color': 'white'}),
        
        # Dropdown menus
        html.Div(
            [
                html.Div([html.Label("Pareto Curve"),
                          dcc.Dropdown(id="dataextent-dropdown", options=[{"label": s, "value": s} for s in ['All Data', 'Pareto Curve Data']], style={'color': 'black'},
                            className="dropdown")], style={'width=':'50%', "margin-left": "8px", 'color': 'white'}),
                
                html.Div([html.Label("Plan Names"),
                          dcc.Dropdown(id="PlanNames-dropdown", options=[{"label": y, "value": y} for y in my_paretos['UniqueDecisions']], style={'color': 'black'},
                                       className="dropdown")], style={'width=':'50%', "margin-left": "8px", "margin-top": "15px", 'color': 'white'}),
            ],
            className="row"),
        
        
        # dcc.Slider(
        #     "generation-slider",
        #     min=0,
        #     max=my_paretos["MaxGeneration"],
        #     step=None,
        #     marks={gen: str(gen) for gen in range(5, my_paretos["MaxGeneration"] + 1,5)},
        #     value=my_paretos["MaxGeneration"],),
        
        
        # X-Y extent controls
        html.Div([html.Label('X-max:'),
                  dcc.Input(id='X_max', placeholder='Enter input here...', value=None, type='number',
                            style={'width': '10%', 'textAlign': 'center'}),
                  html.Div(id='bucket_out1')], style={'display' :'flex', 'justifyContent':'left',"margin-left": "8px","margin-top": "15px", 'color': 'white'}),
        
        html.Div([html.Label('X-min:'),
                  dcc.Input(id='X_min', placeholder='Enter input here...', value=None, type='number',
                            style={'width': '10%', 'textAlign': 'center'}),
                  html.Div(id='bucket_out2')], style={'display' :'flex', 'justifyContent':'left',"margin-left": "8px","margin-top": "15px", 'color': 'white'}),
        
        html.Div([html.Label('Y-max:'),
                  dcc.Input(id='Y_max', placeholder='Enter input here...', value=None, type='number',
                            style={'width': '10%', 'textAlign': 'center'}),
                  html.Div(id='bucket_out3')], style={'display' :'flex', 'justifyContent':'left',"margin-left": "8px","margin-top": "15px", 'color': 'white'}),
        html.Div([html.Label('Y-min:'),
                  dcc.Input(id='Y_min', placeholder='Enter input here...', value=None, type='number',
                            style={'width': '10%', 'textAlign': 'center'}),
                  html.Div(id='bucket_out4')], style={'display' :'flex', 'justifyContent':'left',"margin-left": "8px","margin-top": "15px", 'color': 'white'}),
        
        # Multiple Paretos Checkbox
        
        html.Div(dcc.Checklist(id='my_checklist',
            options=[{'label':dataset, 'value': dataset} for dataset in analysis_file_dir],#[{'label' : 'First Pareto','value': 'FP'}, {'label' : 'Second Pareto','value': 'SP'}],#,'disabled' : True}],
            value = [list(analysis_file_dir.keys())[0]]), style={'display' :'flex', 'justifyContent':'left',"margin-left": "8px","margin-top": "15px", 'color': 'white'} ),
    
        
        # Plot
        html.Div(dcc.Graph(id="Pareto Analysis"), style={"margin-top": "15px"} , className="chart"),
    
        ],
        className="container",style={'backgroundColor':'black'},)


@app.callback(
    Output("Pareto Analysis", "figure"),
    # Input("generation-slider", "value"),
    Input("dataextent-dropdown", "value"),
    Input("PlanNames-dropdown", "value"),
    Input("X_max", "value"),
    Input("X_min", "value"),
    Input("Y_max", "value"),
    Input("Y_min", "value"),
    Input("my_checklist", "value"),
    # Input("MPs", "value"),
    # Input("additionalcategory-dropdown", "value"),
)
def update_figure(dataextent, plannames, X_max, X_min, Y_max, Y_min, my_checklist):#, gener): #,additionalcategory):
        
    # for i in range(1, TPr+1):
    #     exec(f'filtered_dataset{i} = my_paretos["df{i}"]')
    def filter_dataset(data2filter, dataextent, X_max, X_min, Y_max, Y_min, objectives):
        filtered_dataset = data2filter.copy()
    

        if dataextent:
            if dataextent=='Pareto Curve Data':
                filtered_dataset = filtered_dataset[filtered_dataset.Pareto < 1]

        
        # =============== Subsetting graph ==================
        if X_max:
            filtered_dataset=filtered_dataset[filtered_dataset[objectives[1]] <= X_max]
            
        if X_min:
            filtered_dataset=filtered_dataset[filtered_dataset[objectives[1]] >= X_min]
            
        if Y_max:
            filtered_dataset=filtered_dataset[filtered_dataset[objectives[0]] <= Y_max]
            
        if Y_min:
            filtered_dataset=filtered_dataset[filtered_dataset[objectives[0]] >= Y_min]

        return filtered_dataset
     
    newData = []
    fig = go.Figure()
    nn=len(my_checklist)
    cbar=px.colors.qualitative.Light24
    if len(my_checklist) == 0:
        pass
    # elif len(my_checklist) == 1:
    #     filtered_dataset = filter_dataset(my_paretos[my_checklist[0]],my_checklist[0], dataextent, X_max, X_min, Y_max, Y_min, my_paretos["Obj_columns" + my_checklist[0]])
    #     fig1 = px.scatter(filtered_dataset, 
    #                         x=my_paretos['Obj_columns'+my_checklist[0]][1], y=my_paretos['Obj_columns'+my_checklist[0]][0],
    #                         size="Generation", color=plannames if plannames in filtered_dataset.columns else None,hover_name=Variable_for_Trace,
    #                         log_x=False, size_max=5,render_mode="webgl") 
    #     fig = go.Figure(data=fig1.data)
    else:
        for dataset in my_checklist:
            filtered_dataset = filter_dataset(my_paretos[dataset], dataextent, X_max, X_min, Y_max, Y_min, my_paretos["Obj_columns" + dataset])
            
            if plannames:
                groups=filtered_dataset[plannames].unique()
                cbar_legend=dict(zip(groups,px.colors.qualitative.Bold[:len(groups)]))
                
                
                for group in groups:
                    df_group = filtered_dataset[filtered_dataset[plannames] == group]
                    p_x = list(df_group[my_paretos["Obj_columns" + dataset][1]])
                    p_y = list(df_group[my_paretos["Obj_columns" + dataset][0]])
                    fig.add_traces({'type': 'scatter', 
                                    'x': p_x, 'y': p_y,
                                    'name': group, 
                                    # 'ids': filtered_dataset.Pareto.astype(str),  
                                    # # 'marker_color': list( map(cbar_legend.get, unique_decisions) ), #if plannames != None and plannames in  filtered_dataset.columns else None,
                                    'mode':'markers',
                                    'marker_color': cbar_legend.get(group),
                                    })
                
                fig.update_traces(legendgroup=dataset,
                                  showlegend=True)
            
            
            # if plannames in list(filtered_dataset.columns):
            #     facet_ = list(set(filtered_dataset[plannames])).sort()
            #     # print('length facet_ {}'.format(facet_))
            #     for face in facet_:
            #         face_dataset_pareto = filtered_dataset[filtered_dataset.apply(lambda x: x[plannames] == face and int(x['Pareto']) == 0, axis = 1)]
            #         face_dataset = filtered_dataset[filtered_dataset.apply(lambda x: x[plannames] == face and int(x['Pareto']) > 0, axis = 1)]
            #         fig.add_traces({'type': 'scatter', 
            #                 'x': face_dataset[my_paretos['Obj_columns'+dataset][1]], 'y': face_dataset[my_paretos['Obj_columns'+dataset][0]],
            #                 'name': dataset+' '+face, 'ids': face_dataset.Pareto.astype(str),  
            #                 'customdata': face_dataset[plannames].astype(str), #if plannames != None and plannames in  filtered_dataset.columns else None,
            #                 'mode': 'markers', 'opacity': 0.4
            #                 })
            #         fig.add_traces({'type': 'scatter', 
            #                 'x': face_dataset_pareto[my_paretos['Obj_columns'+dataset][1]], 'y': face_dataset_pareto[my_paretos['Obj_columns'+dataset][0]],
            #                 'name': dataset+' '+face, 'ids': face_dataset_pareto.Pareto.astype(str),  
            #                 'customdata': face_dataset_pareto[plannames].astype(str), #if plannames != None and plannames in  filtered_dataset.columns else None,
            #                 'mode': 'markers', 'opacity': 1
            #                 })
            else:
                face_dataset_pareto = filtered_dataset[filtered_dataset['Pareto']<1]
                p_x = list(face_dataset_pareto[my_paretos["Obj_columns" + dataset][1]])
                p_y = list(face_dataset_pareto[my_paretos["Obj_columns" + dataset][0]])
                # print('{}, {}, {}'.format(len(face_dataset_pareto), len(p_x), len(p_y)))
                face_dataset = filtered_dataset[filtered_dataset['Pareto']>0]
                for x in range(4):
                    gen_ = my_paretos["MaxGeneration"]/(x+1)
                    genNext_ = gen_-my_paretos["MaxGeneration"]/4
                    gen_face_ = (face_dataset['Generation']<gen_)&(face_dataset['Generation']>=genNext_)
                    gen_face_dataset = face_dataset[gen_face_]
                    fig.add_traces({'type': 'scatter', 
                            'x': gen_face_dataset[my_paretos['Obj_columns'+dataset][1]], 'y': gen_face_dataset[my_paretos['Obj_columns'+dataset][0]],
                            'name': dataset, 'ids': face_dataset.Pareto.astype(str),  
                            'customdata': None, #if plannames != None and plannames in  filtered_dataset.columns else None,
                            'mode': 'markers', 'opacity': x/4+0.1,#0.4, 
                            'marker_size': x+1, 'marker_color': 'grey'
                            })
                fig.add_traces({'type': 'scatter', 
                            'x': p_x, 'y': p_y,
                            'name': dataset, #'ids': 'Pareto', #face_dataset_pareto.Pareto.astype(str),  
                            'customdata': None, #if plannames != None and plannames in  filtered_dataset.columns else None,
                            'mode': 'markers', 'opacity': 1, 'marker_color':cbar[my_checklist.index(dataset)]
                            })

    
    
    
    
    fig.update_layout(plot_bgcolor=colors["background"], paper_bgcolor=colors["background"], font_color=colors["text"])
    if len(my_checklist) != 0:
        fig.update_layout(height=800,width=1500, xaxis1_title = 'Cost', yaxis1_title = 'Flood Penalty') #my_paretos['Obj_columns'+my_checklist[0]][1] ,yaxis1_title = my_paretos['Obj_columns'+my_checklist[0]][0])

    # --------------------------------------- -----------------------------------------------

    return fig


if __name__ == "__main__":
    app.run_server(port=8050,host='127.0.0.4')
    
