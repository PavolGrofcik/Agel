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
		self.logger.debug("Creating Dask Cluster")
		if self.cluster is None:
			self.cluster = LocalCluster(
				n_workers=self.n_workers,
				processes=True,
				threads_per_worker=self.n_threads,
				memory_limit=self.memory_limit
			)
		self.logger.debug("Dask Cluster created")

	def closeDaskCluster(self):
		self.logger.debug("Closing Dask Cluster")
		if self.cluster is not None:
			self.cluster.close()
			self.cluster = None
		self.logger.debug("Dask Cluster closed")


	def loadDaskDataframe(self, filepath=DATA_FILEPATH, blocksize=5e6, **kwargs):
		self.logger.debug("Loading Dask Dataframe")

		if self.cluster is None:
			self.logger.debug("Dask cluster will be initialized")
			self.createDaskCluster()

		with self.cluster.get_client() as client:
			try:
				ddf = dd.read_csv(filepath, header=0, blocksize=blocksize,  # 5MB chunks
								  dtype={'A1Cresult': 'object', 'diag_1': 'object', 'max_glu_serum': 'object'})
				pdf =  ddf.compute()
				return pdf

			except Exception as e:
				self.logger.error(f"Dataframe '{filepath}' load failed!")
				self.logger.error(e)
				raise e
		self.logger.debug("Dask Dataframe successfully loaded")

	def prepareMappingFile(self, filepath=DATA_MAPPING_FILEPATH):
		pass

	def makeDataframeMapping(self, df, df_mapping):
		pass

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
	df_mapping = Dataloader.loadDaskDataframe(filepath=DATA_MAPPING_FILEPATH)

	#############################
	#Perform mapping to diabetes Dataframe
	#############################
	df_mapping = Dataloader.prepareMappingFile(df_mapping)
	#df_diabetes = Dataloader.makeDataframeMapping(df_diabetes, df_mapping)

	#############################
	#Serialize diabetes dataframe
	#############################
	Dataloader.serializeDataframe(df_diabetes, "diabetic_data", type=".csv")

	#############################
	#Shutdown the Cluster
	#############################
	Dataloader.closeDaskCluster()



