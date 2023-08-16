# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 13:53:08 2023

@author: PEREIRJ1 from Lucas Nguyen (Confluency) Script
"""

import yaml

from pyathena import connect
from pyathena.pandas_cursor import PandasCursor

import plotly.express as px
import pandas as pd

import pyathena
pyathena.__version__

#Import Config
# Define data
data = dict(
    awsID = "AKIAX5P5KVMTU65QDLG2",
    awsSecretKey = "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",
    aws_region = "us-east-1",
    subscription = "c83b0abd87574852",
    work_group = 'St.-Louis-MSD-WCS-Platinum-c83b0abd-8757-4852-bb3b-f616fc5114a3',
    bucket = 'production-optimatics-optimizer-vizdata-'
)

# Write YAML file
with open('config.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(data, outfile, default_flow_style=False, allow_unicode=True)
    
    
#Data Getter Class
class OptimizerDP():
    """A class to query data from the Optimizer data pipeline"""
    def __init__(self, job_name, job_id, config_path):
        """Two fields are needed to query data: the job name and the job ID.
        The job name is `filename` in the Optimizer dashboard. The ID is found by
        clicking on the Aperature page and copying the last part of the URL.
        
        Parameters
        ----------
        job_name: str
            The `filename` in the Optimizer dashboard.
        job_id: str
            The last part of Aperature page URL
        config_path: path-like
            Path to project configuration yaml file
        
        """
        self.job_name = job_name
        self.job_id = job_id
        self.config_path = config_path
        self.config = self._read_config(config_path)
        
        self._cursor = None
        self._cnn_string = None
        
        self._pareto = None
        self._members = None
        
    def _read_config(self, path):
        """Helper to load config yaml file"""
        with open(path, 'r') as stream:
            return yaml.safe_load(stream)
        
    def _optimizer_pd_cursor(self, job_name, job_id, config):
        """Helper to load pd.cursor and optimization connection string"""
        new_job_name = (''.join(char if char.isalnum() else '_' for char in job_name)
                        .lower()
                        #.strip('_')
                        # do we need a recursive replace here?
                        #.replace('__', '_')
                       )
        new_job_id = job_id.replace('-', '')
        cnn_string = config['subscription'] + '_' + new_job_name + '_' + new_job_id
        bucket_name = config['bucket'] + config['aws_region']
        s3_staging_dir = 's3://' + bucket_name + '/' + cnn_string + '/staging'

        cursor =  connect(
            aws_access_key_id=config['awsID'],
            aws_secret_access_key=config['awsSecretKey'],
            s3_staging_dir=s3_staging_dir,
            region_name=config['aws_region'],
            cursor_class=PandasCursor,
            work_group=config['work_group']
        ).cursor()

        return cursor, cnn_string
    
    def query(self, query=None, table=None):
        """Query results from the data pipeline.
        
        Parameters
        ----------
        query: str
            SQL query to S3. 
        table: str
            Specify a table to get all data from. Equal to:
            `SELECT * FROM table`
        """
        if query is None:
            query = f"SELECT * FROM \"{self.cnn_string}\".\"{table}\""

        return self.cursor.execute(query).as_pandas()
    
    @property
    def cursor(self):
        """Pandas Cursor Object."""
        if self._cursor is None:
            cursor, cnn_string = self._optimizer_pd_cursor(self.job_name, self.job_id, self.config)
            self._cursor = cursor
            self._cnn_string = cnn_string
        return self._cursor
            
    @property
    def cnn_string(self):
        """Job connection string."""
        if self._cnn_string is None:
            cursor, cnn_string = self._optimizer_pd_cursor(self.job_name, self.job_id, self.config)
            self._cursor = cursor
            self._cnn_string = cnn_string
        return self._cnn_string
    
    @property
    def results_pareto(self):
        """Pareto Table."""
        if self._pareto is None:
            self._pareto = self.query(table='pareto')
        return self._pareto
    
    @property
    def results_members(self):
        """members Table."""
        if self._members is None:
            self._members = self.query(table='members')
        return self._members
    

#----------------------------------------------------------------------------------------------------------------------------------------

#QUERYING SOME RESULTS

name = "IC.3.STL.Control_SMT"
identifier = "11d3010a-ee18-4c4d-b53c-95959f2e6e1d"
dp_modulated = OptimizerDP(name, identifier, "config.yaml")
dp_modulated.results_pareto

#Get list of results to save
query = f"SHOW DATABASES"
print(query)
dbs = dp_modulated.query(query=query)
dbs = pd.DataFrame(dbs)
dbs["database_name"] = dbs["database_name"].str.split("_", n = 1, expand = True)[1]
dbs_list = dbs["database_name"].str.rsplit("_", n = 1, expand = False)
print(dbs_list[0])
dbs_list[0][0]

#Search for run name (truncated to 40 characters) and identifier (found in url of Aperture dashboard)
name = "ic_11_stl_control_smt_18pc_stodes_tribcs"
identifier = "a0e9496c-ad55-4278-91f3-ea0daa7c4cbf"
dp = OptimizerDP(name, identifier, "config.yaml")
#ic_9_stl_control_smt_18pc_stopeakdwf_tri_fc88194d-be3a-408e-a5d7-30b85736bcb0

p = dp.query(table = "members")
p["name"] = "IC.11.STL.Control_SMT_18PC_StoDes_TribCSO_2Triggers_TYJun23"
location = r"C:\Users\PereirJ1\Jacobs\MSD Optimization - Task 1 OPTIMIZATION\Model\Optimizer\Interim\PreliminaryOptimization\Results\RawData"
filename = "IC.11.STL.Control_SMT_18PC_StoDes_TribCSO_2Triggers_TYJun23.csv"
p.to_csv(location + '\\' + filename, sep = ',', index = False)

