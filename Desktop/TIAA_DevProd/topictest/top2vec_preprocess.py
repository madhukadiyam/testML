#Data Preprocessing

import re
import numpy as np
import pandas as pd
import time

#Importing config file
import top2vec_config as config_list

# remove numbers spaces
def number_space_removal(text):
    review = re.sub("\d+", "", str(text))
    
    return review

# remove junks
def junks_removal(text):
    review = re.sub(r"\W", " ", str(text))
    review = review.lower()
    review = re.sub(r"\s+[a-z]\s+", " ", review)
    review = re.sub(r"^[a-z]\s+", " ", review)
    review = re.sub(r"\d+", " ", review)
    review = re.sub(r"\s+", " ", review)
    
    return review

class top2vec_preprocess:
    def __init__(self):
        self.project_id = config_list.project_id #GCP project id
        self.bigquery_table = config_list.bigquery_table #Bigquery table
        self.gcp_input_file = config_list.gcp_input_file
        self.gcp_top2vec_model = config_list.gcp_top2vec_model# top2vec saved model path
        # self.df = df

    def preprocess_data(self, df):
        df['clean_text'] = df.ABSTRACT.apply(number_space_removal) # Provide your column name for raw text data( in my case, ABSTRACT)
        df['clean_text'] = df.clean_text.apply(junks_removal)

        return df