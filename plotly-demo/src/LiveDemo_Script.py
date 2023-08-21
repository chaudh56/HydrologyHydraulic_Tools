# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:21:33 2023

@author: CHAUDHS1
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import plotly.express as px
import pandas as pd

df = pd.read_csv(r"C:\Users\pb062627\Documents\GitHub\HydrologyHydraulic_Tools\Optimatics_Pareto_Dashboarding\data/RawData.csv")

app = dash.Dash(__name__)

app.layout = html.Div(    
    [ 	html.H1("Country Stats",style={"color":"green","text-align":"center"}),
                            
                        #  Dropdowns ============
                            html.Div(                               
                                [html.Div([html.Label("Developing Status"),
                          				  dcc.Dropdown(id="status-dropdown",
                                                      options=[{"label":y, "value":y} for y in df.Status.unique()],
                          				  className="dropdown")]),
                                  
                                    html.Div([html.Label("Average Schooling"),
                            				  dcc.Dropdown(id="schooling-dropdown",
                                                        options=[{"label":s, "value":s} 
                                                                  for s in range(int(df.Schooling.min()),int(df.Schooling.max())+1)],
                            				  className="dropdown")])
                                  ]),
                        #Figure ===========
                            html.Div(dcc.Graph(id='graph_life_gdp',className="chart"))],className="container",style={})
                                            
                                            
                  
@app.callback(Output("graph_life_gdp", "figure"), 
              Input("status-dropdown", "value"),
              Input("schooling-dropdown", "value"),)


def update_figure(country_status,schooling):
    filtered_df = df.copy()
    
    if schooling:
        filtered_df = filtered_df[filtered_df.Schooling <= schooling]
        
    if country_status:
        filtered_df = filtered_df[filtered_df.Status==country_status]
    
    fig = px.scatter(
        filtered_df,
        x="GDP",
        y="Life expectancy",
        color="continent",
        size="Population",
        hover_name="Country",
        log_x=True,
        size_max=60)
    return fig

if __name__ == "__main__":
    app.run_server(port=8050,host='127.0.0.5')
