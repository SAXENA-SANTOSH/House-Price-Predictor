import os
import pickle
import pandas as pd
from Vaildation.File_validation import File_Validation
from Vaildation.File_Data_validation import File_Data_validation
from Vaildation.Adding_Database import Database_insertion
from Model_Training.Data_transformation import Data_Transformation
from Model_Training.Data_preprocessing import Data_preprocessing
from Model_Training.Model_training import ML_model


class Training:

    def training_pipeline(self, logger, path):
        
        obj = File_Validation(logger,path)
        obj.file_validation_main()

        obj = File_Data_validation(logger , path)
        obj.file_data_validation_main()

        obj = Database_insertion(logger, path , "house","house_data")
        obj.adding_database_pipeline()

        obj = Data_Transformation(logger , path)
        obj.data_transformation_main()

        obj = Data_preprocessing(logger, path)
        obj.data_preprocessing_pipeline()

        for i in os.listdir( path +"/Input_files/Cluster_files"):
            k = i.split("_")[0]
            logger.log("Check",str(k) + " Cluster")
            df = pd.read_csv(path+"/Input_files/Cluster_files/" + i)
            if(df.shape[0] < 20):
                logger.log("Check","Less number of samples hence rejected treated as Outlier")
                continue
            else:
                obj = ML_model(logger, df, path )
                obj.ML_training_pipeline(k)
        

