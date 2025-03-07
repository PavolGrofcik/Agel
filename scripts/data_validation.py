########################################################
# IMPORTS
########################################################
import os

########################################################
# GLOBAL VARIABLES
########################################################
AGEL_SCRIPT_DIR = "/home/pavol/Plocha/Agel/scripts"


########################################################
# DATA VALIDATION START
########################################################
if __name__ == "__main__":
	#1. Change path
	os.chdir(AGEL_SCRIPT_DIR)

	#2. Get logger
	from data_logger import getLogger
	logger = getLogger("data_validation")


	#3.VALIDATE DATA
	with open("validation.txt", "w") as file:
		logger.debug("File opened")
		file.write("Validated!")
	logger.debug("File closed")
