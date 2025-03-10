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

DATA_FILEPATH = f"{AGEL_SCRIPT_DIR}/data_transformed/"
DATA_FILENAME = f"data_transformed"
DATA_FORMAT = ".csv"


########################################################
# DATATRANSFORMER CLASS
########################################################
class DataValidator:
    def __init__(self, logger):
        self.logger = logger
        self.dataframe = None

    def load_dataframe(self, data_dir="/data_transformed", data_name="data_transformed", type=".csv"):
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

    def validate_dataframe(self):
        self.logger.info("Dataframe is to be validated!")
        #perform validation test and integrity tests
        #check encoding categorical columns if are encoded correctly
        #check data integrity - if any medication is prescribed but is_prescibed is false e.g....

        self.logger.info(f"Dataframe successfully validated!")





if __name__ == "__main__":
    #############################
    # Load logger for logging all changes
    #############################
    from data_logger import getLogger

    logger = getLogger("data_validation")
    logger.debug("\n\n###############################\n")

    #############################
    # Change path for Python script
    #############################
    os.chdir(os.path.expanduser(AGEL_SCRIPT_DIR))

    #############################
    # DataValidator init
    #############################
    DataValidator = DataValidator(logger)
    DataValidator.load_dataframe(DATA_FILEPATH, DATA_FILENAME, DATA_FORMAT)
    DataValidator.validate_dataframe()



