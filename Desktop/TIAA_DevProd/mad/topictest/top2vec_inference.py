import re
import numpy as np
import pandas as pd
import pandas_gbq
from pprint import pprint
import time

# Top2Vec for topic modeling
from top2vec import Top2Vec

#Importing config file
import top2vec_config as config_list

#Importing utils
from top2vec_preprocess import top2vec_preprocess
import top2vec_utils

from tqdm import tqdm
import logging
logging.basicConfig(filename= config_list.log_file, level=logging.DEBUG)

def prediction():

    # Loading the model
    try:
        top2vec_model = Top2Vec.load(config_list.gcp_top2vec_model)
        logging.info('Models are loaded successfully')
    except Exception as e:
        logging.error('Unable to load the models..'+ str(e))
        exit()
            
    # Loading data
    try:
        df = top2vec_utils.data_loader().load_from_gcs()
        logging.info('Data has been loaded into dataframe...')
    except Exception as e:
        logging.error('Unable to load the data to dataframe..'+ str(e))
        exit()

    #Data Preprocessing
    try:
        processed_df = top2vec_preprocess().preprocess_data(df)
        logging.info('Data preprocesising completed')
    except Exception as e:
        logging.error('Unable to preprocess the data..'+ str(e))
        exit()

    # Making the corpus ready for prediction
    # Term Document Frequency
    try:
        corpus = processed_df['clean_text']
        doc_ids_previous = len(top2vec_model.doc_id2index)
        top2vec_model.add_documents(corpus)
        doc_ids_updated = len(top2vec_model.doc_id2index)
        logging.info('Model prediction completed')
    except Exception as e:
        logging.error('Unable to perform the prediction..'+ str(e))
        exit()

    # Output tagging to the data
    doc_ids = []
    for i in range(doc_ids_previous, doc_ids_updated):
        doc_ids.append(i)
    top2vec_pediction = top2vec_model.get_documents_topics(doc_ids)

    # adding output columns to dataframe
    processed_df['Dominant_Topic'] = top2vec_pediction[0]
    processed_df['Perc_Contribution'] = top2vec_pediction[1]

    df['Keywords'] = np.nan
    keywords = []
    for keyword in top2vec_pediction[2]:
        keywords.append(keyword)   
    df['Keywords'] = keywords

    # processed_df.to_csv("top2vec_test_out.csv", index = None) #To Save the result in local

    #Save the output dataframe to bigquery
    # pandas_gbq.to_gbq(df, "df_cx_insight_test.top2vec_table", project_id="df-cx-345207", if_exists="append")

    print("******Activity completed *******")

    return processed_df.to_json()
