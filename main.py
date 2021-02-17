from Training import Training
from Logger.Activity_logger import Activity_logger

path = "/Users/santoshsaxena/Desktop/wafer"
        
logger = Activity_logger(path)

obj = Training()
obj.training_pipeline(logger, path)