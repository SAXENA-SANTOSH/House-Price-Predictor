"""
1) missing values with imputer
   Label Encoder
2) separating with x and y
2) standard scaler
3) variance inflation factor / Dimensionality reduction
3) clustering number of clusters

"""


import os
import shutil
import pickle
import numpy as np 
import pandas as pd
from sklearn.preprocessing import LabelEncoder , StandardScaler

class Data_Transformation:

    def __init__(self, logger , path):
        self.path = path
        self.logger = logger
        self.df = pd.read_csv(self.path + "/Input_files/Input_data.csv")
        self.logger.log("Check","Data Transformation Initialized")

    def recovering_missing_values(self):

        try:
            self.logger.log("Check", "Recovering Missing values Initialized")
            self.df[self.df.columns[self.df.dtypes == "object"]] = self.df[self.df.columns[self.df.dtypes == "object"]].replace(["NULL_values"], np.nan)
            self.df[self.df.columns[self.df.dtypes == "int"]] = self.df[self.df.columns[self.df.dtypes == "int"]].replace([9867], np.nan)
            self.df[self.df.columns[self.df.dtypes == "float"]] = self.df[self.df.columns[self.df.dtypes == "float"]].replace([9867.0], np.nan)

            self.df[self.df.columns[self.df.dtypes == "object"]] = self.df[self.df.columns[self.df.dtypes == "object"]].fillna(method = "ffill")
            self.df[self.df.columns[self.df.dtypes == "int"]] = self.df[self.df.columns[self.df.dtypes == "int"]].fillna(self.df.mean())
            self.df[self.df.columns[self.df.dtypes == "float"]] = self.df[self.df.columns[self.df.dtypes == "float"]].fillna(self.df.mean())
            self.df.drop(["Id"], axis = 1 , inplace = True)
            self.logger.log("Done ", "Recovering Missing values Completed")

        except Exception as e:
            self.logger.log("Error", e)

    def generating_files(self):

        try:
            self.logger.log("Check", "Generating files Initialized")
            if(os.path.isdir(self.path + "/Model_files")):
                shutil.rmtree(self.path + "/Model_files")

            os.mkdir(self.path + "/Model_files")
            os.mkdir(self.path + "/Model_files/Encoder")
            os.mkdir(self.path + "/Model_files/Scaler")
            self.logger.log("Done ", "Generating files Completed")

        except Exception as e:
            self.logger.log("Error",e)



    def label_encoding(self):

        try:
            self.logger.log("Check", "Label Encoding Initialized")

            for i in self.X.columns[self.X.dtypes == "object"]:
                encoder = LabelEncoder()
                self.X[i] = encoder.fit_transform(self.X[i])
                pickle.dump(encoder , open(self.path + "/Model_files/Encoder/"+i+"_encoder.pickle" , "wb"))
            self.logger.log("Done ", "Label Encoding Completed")
        
        except Exception as e:
            self.logger.log("Error" , e)

    def spliting_into_x_y(self):

        try:
            self.df.index = range(self.df.shape[0])
            self.logger.log("Check", "Splitting X and y Initialized")
            self.X = self.df.drop(["SalePrice"], axis = 1)
            self.columns = self.X.columns
            self.y = self.df[["SalePrice"]]
            self.logger.log("Done ","Spliiting X and y Completed")
        except Exception as e:
            self.logger.log("Error",e)
    def scaling_values(self):

        try:
            self.logger.log("Check", "Scaling values Initialized")
            sc_x = StandardScaler()
            con = self.df.columns[self.df.dtypes != "object"]
            cat = self.df.columns[self.df.dtypes == "object"]

            result = sc_x.fit_transform(self.df[con])
            result = pd.DataFrame(result, columns=con)

            self.df[self.df.columns[self.df.dtypes != "object"]] = result
            
            pickle.dump(sc_x , open(self.path+"/Model_files/Scaler/scaler_x.pickle" , "wb"))
            self.logger.log("Done ", "Scaling values Completed")

        except Exception as e:
            self.logger.log("Error", e)

    def exporting_processed_file(self):

        try:
            self.X = pd.DataFrame(self.X , columns = self.columns)
            self.logger.log("Check","Exporting processed .csv file Initialized")
            self.export_df = self.X.join(self.y)
            self.export_df.to_csv(self.path+"/Input_files/Processed_input.csv", index = False)
            self.logger.log("Done ","Exporting processed .csv file Completed")

        except Exception as e:
            self.logger.log("Error", e)

    def data_transformation_main(self):
        self.recovering_missing_values()
        self.generating_files()
        self.scaling_values()
        self.spliting_into_x_y()
        self.label_encoding()
        self.exporting_processed_file()





        






"""
self.df[self.df.columns[self.df.dtypes == "object"]] = self.df[self.df.columns[self.df.dtypes == "object"]].fillna(method = "ffill")
        self.df[self.df.columns[self.df.dtypes == "int"]] = self.df[self.df.columns[self.df.dtypes == "int"]].fillna(self.df.mean())
        self.df[self.df.columns[self.df.dtypes == "float"]] = self.df[self.df.columns[self.df.dtypes == "float"]].fillna(self.df.mean())
"""


        

