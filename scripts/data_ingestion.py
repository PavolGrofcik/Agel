########################################################
# IMPORTS
########################################################
import os
import pandas as pd
import datetime as dt
import dask

import dask.dataframe as dd
from dask.distributed import LocalCluster,client


########################################################
# GLOBAL VARIABLES
########################################################
AGEL_HOME_DIR = "/home/pavol/Plocha/Agel"
AGEL_DATA_DIR = "/home/pavol/Plocha/Agel/data"
AGEL_SCRIPT_DIR = "/home/pavol/Plocha/Agel/scripts"

DATA_FILEPATH = "/home/pavol/Plocha/Agel/data/diabetic_data.csv"
DATA_MAPPING_FILEPATH = "/home/pavol/Plocha/Agel/data/IDS_mapping.csv"


########################################################
# DATALOADER CLASS
########################################################
class DaskDataLoader:
    def __init__(self, logger, n_workers=2, n_threads=1, memory_limit="2GB"):
        self.cluster = None
        self.client = None
        self.n_workers = n_workers
        self.n_threads = n_threads
        self.memory_limit = memory_limit

        self.ddf = None
        self.pdf = None
        self.logger = logger

    def __del__(self):
        if self.cluster is not None:
            self.closeDaskCluster()

    def createDaskCluster(self):
        self.logger.info("Creating Dask Cluster")
        if self.cluster is None:
            self.cluster = LocalCluster(
                n_workers=self.n_workers,
                processes=True,
                threads_per_worker=self.n_threads,
                memory_limit=self.memory_limit
            )
        self.logger.info("Dask Cluster created")

    def closeDaskCluster(self):
        self.logger.info("Closing Dask Cluster")
        if self.cluster is not None:
            self.cluster.close()
            self.cluster = None
        self.logger.info("Dask Cluster closed")


    def loadDaskDataframe(self, filepath=DATA_FILEPATH, blocksize=5e6, cols=None, **kwargs):
        self.logger.info("Loading Dask Dataframe")

        if self.cluster is None:
            self.logger.info("Dask cluster will be initialized")
            self.createDaskCluster()

        self.logger.info("Dask Dataframe is to be loaded")
        with self.cluster.get_client() as client:
            try:
                if cols is None:
                    ddf = dd.read_csv(filepath, header=0, blocksize=blocksize,  # 5MB chunks
                                     dtype={'A1Cresult': 'object', 'diag_1': 'object', 'max_glu_serum': 'object'})
                else:
                    ddf = dd.read_csv(filepath, header=0, blocksize=blocksize, names=cols,# 5MB chunks
                                     dtype={'A1Cresult': 'object', 'diag_1': 'object', 'max_glu_serum': 'object'})
                pdf =  ddf.compute()
                return pdf

            except Exception as e:
                self.logger.error(f"Dataframe '{filepath}' load failed!")
                self.logger.error(e)
                raise e
        self.logger.info("Dask Dataframe successfully loaded")

    def prepareMappingFile(self, df_mapping):
        self.logger.info("Initializing mapping file")

        try:
            #Convert columns to string
            df_mapping['val'] = df_mapping['val'].astype('str')
            df_mapping['desc'] = df_mapping['desc'].astype('str')

            #Create new ad-hoc columns
            df_mapping['val_key'] = df_mapping['val'].apply(lambda x: x if 'id' in x else 0)
            df_mapping['val_header'] = df_mapping['val'].apply(lambda x: 1 if 'id' in x else 0)

            #Make mapping
            df_mapping['val_key'] = df_mapping['val_key'].replace(0, pd.NA).ffill()

        except Exception as e:
            self.logger.error(f"Initializing of mapping file failed!")
            self.logger.error(e)
            raise
        else:
            self.logger.info("Initializing of mapping file successfully finished!")
        finally:
            return df_mapping

    def makeDataframeMapping(self, df, df_mapping):
        self.logger.info("Dataframe mapping starting")

        if df is None or df_mapping is None:
            self.logger.error("Dataframe is None or the mapping file is None!")
            raise Exception("Dataframe is None or the mapping file is None!")

        try:

            if len(df.columns) != 50:
                self.logger.info(f"Dataframe columns already reordered and mapped!")
                return df

            self.logger.info("Creating dicts for mapping values")
            #Create dicts for mapping
            admission_type = df_mapping[(df_mapping.val_key == 'admission_type_id') & (df_mapping.val_header == 0)][
                ['val', 'desc']]
            discharge_df = df_mapping[(df_mapping.val_key == 'discharge_disposition_id') & (df_mapping.val_header == 0)][
                ['val', 'desc']]
            admission_source_df = df_mapping[(df_mapping.val_key == 'admission_source_id') & (df_mapping.val_header == 0)][
                ['val', 'desc']]

            # Initialize dicts for mapping values
            admission_type_id = {}
            discharge_id = {}
            admission_source_id = {}


            self.logger.info("Initializing empty dicts for mapping")
            for i, row in admission_type.iterrows():
                admission_type_id[row['val']] = row['desc']

            for i, row in discharge_df.iterrows():
                discharge_id[row['val']] = row['desc']

            for i, row in admission_source_df.iterrows():
                admission_source_id[row['val']] = row['desc']

            # Make a mapping -> create new description colums based on ID source columns from dictionaries
            self.logger.info("Applying mapping to source dataframe")
            df['admission_type_desc'] = df['admission_type_id'].apply(lambda x: admission_type_id.get(str(x), pd.NA))
            df['discharge_disposition_desc'] = df['discharge_disposition_id'].apply(lambda x: discharge_id.get(str(x), pd.NA))
            df['admission_source_desc'] = df['admission_source_id'].apply(lambda x: admission_source_id.get(str(x), pd.NA))

            self.logger.info("Reordering dataframe columns")
            #Reorder description columns to their ID source columns
            df_cols = df.columns.tolist()

            #Get ID index positions
            admission_id_idx = int(df_cols.index('admission_type_id'))
            discharge_id_idx = int(df_cols.index('discharge_disposition_id'))
            admission_source_id_idx = int(df_cols.index('admission_source_id'))
            self.logger.debug(f"Dataframe columns: {df_cols}")
            self.logger.debug(f"Dataframe admissions_id_idx: {admission_id_idx}")

            # 12. Reorder columns in a way that id columns contains also their descriptions next to each other
            df_cols = [*df_cols[:admission_id_idx + 1], df_cols[-3], df_cols[discharge_id_idx], df_cols[-2], df_cols[admission_source_id_idx], df_cols[-1],
                       *df_cols[admission_source_id_idx + 1:-3]]

            self.logger.debug(f"Dataframe reordered columns: {df_cols}")

            df = df[df_cols]

        except Exception as e:
            self.logger.error('Dataframe mapping failed!')
            self.logger.error(e)
            raise
        else:
            self.logger.info("Dataframe mapping successfully finished!")
            return df

    def serializeDataframe(self, df, name, type=".csv"):
        self.logger.info(f"Serializing Dataframe to: {name} with saving option: {type}")

        try:
            date_today = dt.date.today().strftime("%Y_%m_%d")

            if type == ".csv":
                df.to_csv(f"{name}_{date_today}.csv", index=False)
            elif type == ".parquet":
                df.to_parquet(f"{name}_{date_today}.parquet")
            elif type == "pickle":
                df.to_pickle(f"{name}_{date_today}.pickle")
            else:
                raise KeyError('Invalid type for serialization of the Dataframe')
        except Exception as e:
            self.logger.error(f"Error during serialization of the Dataframe: {e}")
            raise
        else:
            self.logger.info(f"Serialization of dataframe to: {name} with saving option: {type} successful")


########################################################
# DATA INGESTION START
########################################################
if __name__ == "__main__":
    #############################
    # Change path for Python script
    #############################
    os.chdir(AGEL_SCRIPT_DIR)

    #############################
    # Load logger for logging all changes
    #############################
    from data_logger import getLogger
    logger = getLogger("data_ingestion")
    logger.debug("\n\n###############################\n")


    #############################
    #Dask Cluster loader INIT
    #############################
    Dataloader = DaskDataLoader(logger, n_workers=2, n_threads=1, memory_limit="2GB")
    Dataloader.createDaskCluster()

    #############################
    #Loading Dask Dataframes
    #############################
    df_diabetes = Dataloader.loadDaskDataframe(filepath=DATA_FILEPATH)
    df_mapping = Dataloader.loadDaskDataframe(filepath=DATA_MAPPING_FILEPATH, cols=['val', 'desc'])

    #############################
    #Perform mapping to diabetes Dataframe
    #############################
    df_mapping = Dataloader.prepareMappingFile(df_mapping)
    df_diabetes = Dataloader.makeDataframeMapping(df_diabetes, df_mapping)

    #############################
    #Serialize diabetes dataframe
    #############################
    Dataloader.serializeDataframe(df_diabetes, "diabetic_data", type=".csv")

    #############################
    #Shutdown the Cluster
    #############################
    Dataloader.closeDaskCluster()



