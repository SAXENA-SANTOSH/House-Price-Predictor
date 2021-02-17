"""
This class will set a database and create a table for our dataset. After inserting all the observations in the database
that table will be exported to .csv file and will be stored in input file with the name of Input_data.csv. 

written by Santosh Saxena 
on 11/12/2020


"""



import os
import shutil
import mysql.connector
import numpy as np
import pandas as pd


class Database_insertion:

    def __init__(self, logger , path, database_name, table_name):
        
        """
        This load the necessary path and variables that will be used throughout the code
        """


        self.path = path
        self.logger = logger
        self.db_name = database_name
        self.table_name = table_name
        self.columns = pd.read_csv(self.path+"/Source of Truth/Data_type.csv").iloc[:,0]
        self.dtypes = []
        
        self.logger.log("Check","Database insertion Initialized")
        df = pd.read_csv(self.path+"/Source of Truth/Data_type.csv")
        for i in df.iloc[:,1]:
            if(i == "int64"):
                self.dtypes.append("int")
            if(i == "float64"):
                self.dtypes.append("float")
            if(i == "object"):
                self.dtypes.append("varchar")


    def database_connectivity(self):
        
        """
        This function will create a database
        
        Input : N/A
        Output : Database 
        """

        try:
            self.logger.log("Check","Database Connectivity Initialized")
            db = mysql.connector.connect(
                 host ='localhost',
                 user = 'root',
                 password = 'Santoshkyn14@'
            )
            if(db):
                self.logger.log("Done ","Database Connectivity Completed")
            else:
                self.logger.log("Error","Database Connectivity Failed")
        
            cursor = db.cursor()
            cursor.execute("show databases")
            exist_db = False
            for i in cursor:
                if(i[0] == self.db_name):
                    exist_db = True
                    break
                else:
                    continue
            if(exist_db == False):
                cursor.execute("create database  "+ self.db_name)
            
            self.logger.log("Done ","Database Initialized")
        except Exception as e:
            self.logger.log("Error",e)

    def create_table(self, mycursor ,table_name , columns , columns_type , num , features = [], primary_key = [], foreign_key = [], reference = []):
        """
        This is a function to generate a query for creating a table

        Input : features of tables for Database
        Output : Table in database
        """
        
        
        try:
            if(columns == [] or columns_type == [] and mycursor == ""):
                self.logger.log("Error","Please enter attributes for the tables with proper type")
            else:
                string = "create table if not exists " + table_name + "("
                for i,j,k in zip(columns, columns_type, num):
                    if(j == "float"):
                        k = ""
                    else:
                        k = "("+str(k) +")"
                    string = string + i+" "+j  + str(k) 
                    if(features != []):
                        string = string + features.pop(0) + ","
                    else:
                        string = string + ","
                if(primary_key != []):
                    string = string + "primary key(" + primary_key.pop(0) + ")," 
                for i,j in zip(foreign_key, reference):
                    string = string + "foreign key (" + i + ") " + "references " + j+","
                string = string[0:len(string) - 1]
                string = string + ")"
                mycursor.execute(string)
        except Exception as e:
            self.logger.log("Error",e)

    def insert_into_table(self, mycursor,table_name=[], values=[]):
        """
        This is a function to generate a query for insertion of observation into database table 

        Input : observations to add in database
        Output : observation in a database table
        """
        
        try:
            if(table_name == [] or values == []):
                self.logger.log("Error","Please improve the parameter")
            else:
                string = "insert into "+table_name 
                mycursor.execute("desc " + table_name)
                string = string + "("
                for i in mycursor:
                    string = string + i[0] +","
                string = string[0: len(string) - 1]
                string = string + ")"
                string = string + " values("
                for i in values:
                    if(type(i) == str):
                        string = string + "'"
                        string = string + "{}".format(i)
                        string = string + "',"
                    else:
                        string = string + "{}".format(i)
                        string = string + ","
                string = string[0:len(string)-1]
            string = string + ")"
            mycursor.execute(string)
        except Exception as e:
            self.logger.log("Error",e)



    def setting_table(self):

        """
        This function will create table in database

        Input : N/A
        Output : Database table creation function call
        """

        try:
            self.logger.log("Check","Database Initialized")
            self.db = mysql.connector.connect(
                host = "localhost",
                user = "root",
                password = "Santoshkyn14@",
                database = self.db_name
            )
            if self.db:
                self.logger.log("Done ","Database Created")
            else:
                self.logger.log("Error","Database creation failed")

            self.cursor = self.db.cursor()
            exist_table= False
            self.cursor.execute("show tables")
            for i in self.cursor:
                if(i[0] == self.table_name):
                    exist_table = True
                    break
                else:
                    continue


            self.logger.log("Check","Table Creation Initialized")

            if(exist_table):
                self.cursor.execute("drop table "+ str(self.table_name))
            

            
            
            self.create_table(self.cursor ,self.table_name , 
            list(self.columns),
            list(self.dtypes),
            [100]*len(self.columns)
            )  

                 
            self.logger.log("Done ","Table Created Successfully")
        except Exception as e:
            self.logger.log("Error",e)

    
    def inserting_into_database(self):

        """
        This function will insert observations into database tables

        Input : N/A
        Output : Inserting obeservations into database table function call 

        """
        try:
            self.logger.log("Check","Inserting of data into database Initialized")
            for i in os.listdir(self.path+"/Good_Dataset/"):
                df = pd.read_csv(self.path+"/Good_Dataset/"+i)
                df[df.columns[df.dtypes == "int"]] = df[df.columns[df.dtypes == "int"]].fillna(value= 9867)
                df[df.columns[df.dtypes == "float"]] = df[df.columns[df.dtypes == "float"]].fillna(value = 9867.0)
                df[df.columns[df.dtypes == "object"]] = df[df.columns[df.dtypes == "object"]].fillna(value= "NULL_values")
                for j in range(len(df)):
                    self.insert_into_table(mycursor = self.cursor,table_name= self.table_name,values=list(df.iloc[j]))

            self.logger.log("Done ","Insertion of data on database is Completed")
            self.logger.log("Check", "Exporting to .csv Initialized")

            self.cursor.execute("select * from "+self.table_name)
            input_data = self.cursor.fetchall()

            self.db.commit()

            
            if(os.path.isdir(self.path + "/Input_files")):
                shutil.rmtree(self.path + "/Input_files")
            
            os.mkdir(self.path + "/Input_files")

            column = open(self.path+"/Source of Truth/Data_columns.txt","r")
            input_dataframe = pd.DataFrame(input_data, columns= column.read().split(","))
            input_dataframe.to_csv(self.path + "/Input_files/Input_data.csv", index = False)
            self.logger.log("Done ", "Exporting to .csv Completed")

        except Exception as e:
            self.logger.log("Error", e)

    def adding_database_pipeline(self):

        """
        This function is main body for the entire class 

        Input : N/A
        Output : Execution of pipeline
        """

        self.database_connectivity()
        self.setting_table()
        self.inserting_into_database()