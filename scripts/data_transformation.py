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

	with open("transformation.txt", "w") as file:
		file.write("Transformed!")
