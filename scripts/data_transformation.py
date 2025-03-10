########################################################
# IMPORTS
########################################################
import os
import numpy as np
import pandas as pd
import datetime as dt
import dask
import requests

import dask.dataframe as dd
from dask.distributed import LocalCluster,client

from airflow.models import Variable
########################################################
# GLOBAL VARIABLES
########################################################
AGEL_DIR = Variable.get("AGEL_DIR")
AGEL_DIR = f"~/{AGEL_DIR}"

AGEL_DATASET_URL = Variable.get("DATASET_URL")


AGEL_HOME_DIR = f"{AGEL_DIR}"
AGEL_DATA_DIR = f"{AGEL_DIR}/data"
AGEL_SCRIPT_DIR = f"{AGEL_DIR}/scripts"

DATA_FILEPATH = f"{AGEL_SCRIPT_DIR}/data_ingested/"
DATA_FILENAME = f"data_ingested"
DATA_FORMAT = ".csv"

DATA_TRANSFORMED_FILEPATH = f"data_transformed"
DATA_TRANSFORMED_FILENAME = "data_transformed"
DATA_TRANSFORMED_FORMAT = ".csv"

########################################################
# DATATRANSFORMER CLASS
########################################################
class DataTransformer:
    def __init__(self, logger):
        self.logger = logger
        self.dataframe = None

    def load_dataframe(self, data_dir="/diabetes_data", data_name="diabetes_data", type=".csv"):
        date_today = dt.date.today().strftime("%Y_%m_%d")

        try:
            self.logger.info(f"Data: {data_dir}{data_name}_{date_today}{type} is to be loaded")
            self.dataframe = pd.read_csv(f"{data_dir}{data_name}_{date_today}{type}")
            self.logger.info(f"Data: '{data_dir}{data_name}_{date_today}_{type}' successfully loaded")

        except FileNotFoundError:
            self.logger.error(f"File {data_dir}{data_name}_{date_today} not found")
            self.logger.error("Please specify correctly dataset file to be saved!")
            raise FileNotFoundError
        except Exception as e:
            self.logger.error(f"Failed to load dataset {data_dir}{data_name}_{date_today}")
            self.logger.error(e)
            raise

    def transform_dataframe(self, dataframe):
        self.logger.info("Dataframe is to be transformed!")

        self.dataframe['max_glu_serum'] = self.dataframe['max_glu_serum'].astype(str)
        self.dataframe['max_glu_serum'] = self.dataframe['max_glu_serum'].map(lambda x: str(x).lower())
        cols_unique = self.dataframe.max_glu_serum.unique().tolist()
        self.dataframe['max_glu_serum_enc'] = self.dataframe['max_glu_serum'].replace(to_replace=cols_unique, value=np.arange(len(cols_unique)).tolist())

        #do transformation... -> encode columns to_categorical
        #E.G
        # column_name = df.column.name
        #col_uniq = df.column.unique().tolist()
        #df[f'{column_name}_enc'] = df.column_name.replace(to_replace=col_uniq, value=np.arange().tolist())


    def serializeDataframe(self, destination_path="/preprocessed", name="diabetes_data", type=".csv"):
        self.logger.info(f"Serializing dataframe to: {destination_path}/{name} with saving option: {type}")

        try:
            if self.dataframe is None:
                self.logger.error("Dataframe to transform is not loaded!")
                raise

            date_today = dt.date.today().strftime("%Y_%m_%d")
            self.logger.info(f"Initializing destination directory: {destination_path}")

            if not os.path.exists(destination_path):
                os.makedirs(destination_path)
                self.logger.info(f"Destination dir: {destination_path} not existing! Will be created")
                self.logger.info(f"Destination dir: {destination_path} creation sucessfull!")

            if type == ".csv":
                self.dataframe.to_csv(f"{destination_path}/{name}_{date_today}.csv", index=False)
            elif type == ".parquet":
                self.dataframe.to_parquet(f"{destination_path}/{name}_{date_today}.parquet")
            elif type == "pickle":
                self.dataframe.to_pickle(f"{destination_path}/{name}_{date_today}.pickle")
            else:
                raise KeyError('Invalid type for serialization of the Dataframe')
        except Exception as e:
            self.logger.error(f"Error during serialization of the Dataframe: {e}")
            raise
        else:
            self.logger.info(f"Serialization of dataframe to: {name} with saving option: {type} successful")



if __name__ == "__main__":
    #############################
    # Load logger for logging all changes
    #############################
    from data_logger import getLogger

    logger = getLogger("data_transformation")
    logger.debug("\n\n###############################\n")

    #############################
    # Change path for Python script
    #############################
    os.chdir(os.path.expanduser(AGEL_SCRIPT_DIR))

    #############################
    # DataTransformer init
    #############################
    DataTransformer = DataTransformer(logger)
    DataTransformer.load_dataframe(data_dir=DATA_FILEPATH, data_name=DATA_FILENAME, type=DATA_FORMAT)

    #############################
    # Making transformation of the dataset
    #############################
    DataTransformer.transform_dataframe(dataframe=DataTransformer.dataframe)
    DataTransformer.serializeDataframe(destination_path=DATA_TRANSFORMED_FILEPATH, name=DATA_TRANSFORMED_FILENAME, type=DATA_TRANSFORMED_FORMAT)