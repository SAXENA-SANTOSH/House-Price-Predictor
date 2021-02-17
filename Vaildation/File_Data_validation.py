"""
This Class will check the data available in files. The major criteria of validation data in the files are -

1) Columns data validation
2) number of row validation
3) data type of features validation
4) unique categorical values from categorical feature

If the data available in the files satisfies these criteria then only it will be remained in the good data
else that file will be moved to bad dataset

created by : Santosh Saxena
on 8/2/2021
"""

import os
import json
import shutil
import pandas as pd
from Logger.Activity_logger import Activity_logger

class File_Data_validation:

    def __init__(self,logger ,path):

        self.logger = logger
        self.path = path
        self.logger.log("Check","File Data Validation Initialized")
        
    def loading_sot(self):

        """
        This function will load the source of truth to compare with our dataset

        Input : N/A
        Output : SOT files dataframe or dictionary 
        """

        try:
            self.logger.log("Check","Loading Source of Truth Initialized")
            column = open(self.path+"/Source of Truth/Data_columns.txt","r")
            self.column = column.read().split(",")
            column.close()
            self.categorical_unique_values = json.load(open(self.path+"/Source of Truth/Data_categorical.json","r"))
            self.file_validation = json.load(open(self.path+"/Source of Truth/File_validation.json","r"))
            self.datatype = pd.read_csv(self.path+"/Source of Truth/Data_type.csv", index_col="Unnamed: 0")
            self.logger.log("Done"," Loading Source of Truth Done")
        except Exception as e:
            self.logger.log("Error",e)

    def unique_categorical_values(self, df):

        """
        This function will check if any random categorical values are there which are not present in the source of truth at 
        that time that dataset will move to towards bad dataset

        """
        try:
            fault = 0
            for i in df.columns[df.dtypes == "object"]:
                cat_sot_values = set(self.categorical_unique_values[i])
                cat_df_values = set(pd.Categorical(df[i]).categories)
                if(cat_df_values.issubset(cat_sot_values)):
                    continue
                else:
                    fault += 1
            return fault
        except Exception as e:
            self.logger.log("Error", e)

    def data_type_validation(self, df):

        """
        This function will check the data types of the comming data with respect to the source of truth
        """
        
        try:
            fault = 0
            for i in df.columns:
                if(df[i].dtypes == self.datatype.loc[i]["SOT"]):
                    continue
                else:
                    fault += 1

            return fault
        except Exception as e:
            self.logger.log("Error",e)



    def file_data_validation(self):

        """
        If all the condition are satisfied then only it will be in the present in the good dataset 
        else that file will be moved to bad data folder
        """

        self.logger.log("Check","File data validation Initialized")
        try:
            for i in os.listdir(self.path+"/Good_Dataset/"):
                df = pd.read_csv(self.path+"/Good_Dataset/"+i)
                if(list(df.columns) != self.column):
                    shutil.move(self.path+"/Good_Dataset/"+i , self.path+"/Bad_Dataset/"+i)
                elif(len(df) != self.file_validation["no_of_rows"]):
                    shutil.move(self.path+"/Good_Dataset/"+i , self.path+"/Bad_Dataset/"+i)
                elif(self.data_type_validation(df) != 0):
                    shutil.move(self.path+"/Good_Dataset/"+i , self.path+"/Bad_Dataset/"+i)
                elif(self.unique_categorical_values(df) != 0):
                    shutil.move(self.path+"/Good_Dataset/"+i , self.path+"/Bad_Dataset/"+i)
                else:
                    continue
            self.logger.log("Done ","File data validation Completed")
        except Exception as e:
            self.logger.log("Error", e)

    def file_data_validation_main(self):
        """
        This is the main body for file_data_validation
        """
        self.loading_sot()
        self.file_data_validation()

        
        





            

                
