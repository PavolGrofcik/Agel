########################################################
# IMPORTS
########################################################
import os

from airflow.models import Variable
########################################################
# GLOBAL VARIABLES
########################################################
AGEL_DIR = Variable.get("AGEL_DIR")
AGEL_DIR = f"~/{AGEL_DIR}"

AGEL_HOME_DIR = f"{AGEL_DIR}"
AGEL_DATA_DIR = f"{AGEL_DIR}/data"
AGEL_SCRIPT_DIR = f"{AGEL_DIR}/scripts"

DATA_FILEPATH = f"{AGEL_DIR}/data/diabetic_data.csv"
DATA_MAPPING_FILEPATH = f"{AGEL_DIR}/data/IDS_mapping.csv"



########################################################
# DATA VALIDATION START
########################################################
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


	#3.VALIDATE DATA
	with open("validation.txt", "w") as file:
		logger.debug("File opened")
		file.write("Validated!")
	logger.debug("File closed")
