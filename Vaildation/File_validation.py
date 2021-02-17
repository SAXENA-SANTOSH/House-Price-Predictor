"""
This class will validate file name of the dataset and classify the good files in good folder and bad files in bad folder

written by Santosh Saxena
on 5/2/2021

"""


import os
import re
import json
import shutil
import numpy as np  
from Logger.Activity_logger import Activity_logger

class File_Validation:
    def __init__(self, logger, path):
        
        self.path = path
        self.logger = logger
        self.logger.log("Check","File name validation Initialized")
        self.no_of_date = json.load(open(path+"/Source of Truth/File_validation.json","r"))["no_date"]
        self.no_of_day = json.load(open(path+"/Source of Truth/File_validation.json","r"))["no_day"]


    def generate_directories(self):

        """
        This function will generate 2 directories which are -
        1) good data folder
        2) bad data folder
        """

        try:
            self.logger.log("Check","Generation of directories Initialized")
            if not os.path.isdir(self.path+"/Good_Dataset"):
                os.mkdir(self.path+"/Good_Dataset")
            else:
                shutil.rmtree(self.path+"/Good_Dataset")
                os.mkdir(self.path+"/Good_Dataset")
            if not os.path.isdir(self.path+"/Bad_Dataset"):
                os.mkdir(self.path+"/Bad_Dataset")
            else:
                shutil.rmtree(self.path+"/Bad_Dataset")
                os.mkdir(self.path+"/Bad_Dataset")
            self.logger.log("Done ","Generation of directories Completed")

        except Exception as e:
            self.logger.log("Error" , e)

    def file_structure_checker(self, message):

        """
        This function will check the structure of file name are valid or not.
        """

        file_name = "['House']+['_']+['\d']+['_']+['\d']+[.csv]"
        if re.match(file_name , message):
            return True
        else:
            return False

    def file_name_validation(self):

        """
        This function will check weather file name is valid according to source of truth or not
        """
        try:

            self.logger.log("Check","File name validation Initialized")
            for i in os.listdir(self.path+"/Training_Batch_Folder"):
                file = i
                if self.file_structure_checker(i):
                    if(i[-4::1] == ".csv"):
                        i = i.split(".")[0]
                        i = i.split("_") 
                        if(i[0] == "House" and len(i[1]) == self.no_of_date and len(i[2]) == self.no_of_day):
                            shutil.copy(self.path+"/Training_Batch_Folder/"+file, self.path+"/Good_Dataset")
                        else:
                            shutil.copy(self.path+"/Training_Batch_Folder/"+file, self.path+"/Bad_Dataset")

                    else:
                        shutil.copy(self.path+"/Training_Batch_Folder/"+file , self.path+"/Bad_Dataset")

                else:
                    shutil.copy(self.path+"/Training_Batch_Folder/"+file , self.path+"/Bad_Dataset")
            
            self.logger.log("Done ","File name validation Completed")
        except Exception as e:
            self.logger.log("Error", e)
        
    def file_validation_main(self):

        """
        This is the main body for file_valition
        """
        self.generate_directories()
        self.file_name_validation()