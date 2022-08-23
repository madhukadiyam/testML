#Data Preprocessing

import re
import numpy as np
import pandas as pd
import time

#Importing config file
import top2vec_config as config_list

class data_loader:
    def __init__(self):
        self.project_id = config_list.project_id # GCP project id
        self.bigquery_table = config_list.bigquery_table # Bigquery table
        self.gcp_input_file = config_list.gcp_input_file # Input file

    def load_from_bigquery(self):
        # Reading data from Bigquery
        bigquery_client = bigquery.Client(project = config_list.project_id ) #Defining bigquery client
        # Query to pull the data from bigquery table
        query1 =  "select * from "+config_list.bigquery_table
        query_job1 = bigquery_client.query(query1)
        # Creating pandas dataframe
        df = query_job1.to_dataframe()

        return df

    def load_from_gcs(self):
        # Reading data from cloud storage
        #Pands 0.24 and above version supports to read the data directly in to dataframe from google cloud storage
        df = pd.read_csv(config_list.gcp_input_file) #You have to define your seperation parameter, default in CCAI is "|"

        return df