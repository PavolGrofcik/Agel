########################################################
# IMPORTS
########################################################
import os
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

DATA_PROCESSED_NAME = "diabetes_data"
DATA_PROCESSED_FORMAT = ".csv"

DATA_SAVE_PATH = f"{AGEL_SCRIPT_DIR}/diabetes_data/"
DATA_SAVE_FINAL_PATH = f"processed_data"
DATA_FINAL_NAME = "data_processed"
DATA_FINAL_FORMAT = ".parquet"


########################################################
# DATA SAVER CLASS
########################################################
class DataSaver:
    def __init__(self, logger):
        self.logger = logger
        self.dataframe = None

    def load_dataset(self, data_dir="/preprocessed", data_name="diabetes_data", type=".csv"):
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


    def save_dataset(self, destination_dir="data_processed", data_name="dataset_final", type=".parquet"):
        self.logger.info(f"Saving dataset with name: {data_name} and saving option: {type}")

        try:

            if self.dataframe is None:
                self.logger.info(f"Dataframe not loaded, method will end")
                return

            date_today = dt.date.today().strftime("%Y_%m_%d")

            self.logger.info(f"Initializing destination directory: {destination_dir}")

            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir)
                self.logger.info(f"Destination dir: {destination_dir} not existing! Will be created")
                self.logger.info(f"Destination dir: {destination_dir} creation sucessfull!")

            self.logger.info(f"Initializing destination directory: {destination_dir} sucessfull!")

            self.logger.info(f"Dataset is to be saved at: {destination_dir}/{data_name}_{date_today}{type}!")
            if type == ".csv":
                self.dataframe.to_csv(f"{destination_dir}/{data_name}_{date_today}.csv", index=False)
            elif type == ".parquet":
                self.dataframe.to_parquet(f"{destination_dir}/{data_name}_{date_today}.parquet")
            elif type == "pickle":
                self.dataframe.to_pickle(f"{destination_dir}/{data_name}_{date_today}.pickle")
            else:
                raise KeyError(f'Invalid type for saving of the dataset: {type}')
        except Exception as e:
            self.logger.error(f"Error during saving of the dataset: {e}")
            raise
        else:
            self.logger.info(f"Dataset successfully saved at: {destination_dir}/{data_name}_{date_today}{type}!")


########################################################
# DATA INGESTION START
########################################################
if __name__ == "__main__":


    #############################
    # Load logger for logging all changes
    #############################
    from data_logger import getLogger
    logger = getLogger("data_saving")
    logger.debug("\n\n###############################\n")

    #############################
    # Change path for Python script
    #############################
    os.chdir(os.path.expanduser(AGEL_SCRIPT_DIR))

    #############################
    # Run DataSave class to save the dataset to .parquet file
    #############################
    DataSaver = DataSaver(logger)
    DataSaver.load_dataset(data_dir=DATA_SAVE_PATH, data_name=DATA_PROCESSED_NAME, type=DATA_PROCESSED_FORMAT)
    DataSaver.save_dataset(destination_dir=DATA_SAVE_FINAL_PATH, data_name=DATA_FINAL_NAME, type=DATA_FINAL_FORMAT)
    x = 5