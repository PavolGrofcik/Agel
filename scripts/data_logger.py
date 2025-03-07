########################################################
# IMPORTS
########################################################
import logging
import datetime as dt

########################################################
# GLOBAL VARIABLES
########################################################
AGEL_LOG_DIR = "/home/pavol/Plocha/Agel/logs"

LOGGER_NAME = "log"
LOGGER_DATE_FORMAT = "%Y_%m_%d"
LOGGER_ZONE_OFFSET = dt.timedelta(hours=1) #UTC + 1 HOUR SLOVAKIA
LOGGER_SUFFIX = "log"

LOGGER_LEVEL = logging.DEBUG
LOGGER_ENCODING = "utf-8"
LOGGER_MODE = "a"
LOGGER_MESSAGE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOGGER_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


########################################################
# LOGGER FUNCTIONS
########################################################

#FILENAME: LOG_2025_03_01.log
def getLogger(name: str):
    """
    Method instantiates a logger object for logging
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOGGER_LEVEL)

    try:

        #Logging to File
        datetime_today = dt.datetime.now(tz=dt.timezone(offset=LOGGER_ZONE_OFFSET)) #UTC + 1 HOUR for SLOVAKIA
        datetime_today_str = datetime_today.strftime(LOGGER_DATE_FORMAT)
        formater = logging.Formatter(fmt=LOGGER_MESSAGE_FORMAT,datefmt=LOGGER_DATETIME_FORMAT)
        file_handler = logging.FileHandler(f"{AGEL_LOG_DIR}/{LOGGER_NAME}_{datetime_today_str}.{LOGGER_SUFFIX}",
                                           mode=LOGGER_MODE,encoding=LOGGER_ENCODING)
        file_handler.setFormatter(formater)
        logger.addHandler(file_handler)

    except Exception as e:
        print("Exception occured while creating logger")
    finally:
        return logger


########################################################
# PROGRAM START
########################################################
if __name__ == "__main__":
    pass