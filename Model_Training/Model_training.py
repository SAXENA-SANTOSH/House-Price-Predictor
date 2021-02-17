"""
This class will find the individual model for all clusters. The model with higher accuracy will be selected and 
saved in a particular directory

Written by : Santosh Saxena
on 17/2/2021

"""


import os
import shutil
import pickle
from sklearn.svm import SVR
from sklearn.metrics import r2_score
from xgboost import XGBRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LinearRegression , Ridge , Lasso
from sklearn.model_selection import GridSearchCV , train_test_split
from sklearn.ensemble import BaggingRegressor , RandomForestRegressor
from sklearn.ensemble import AdaBoostRegressor , GradientBoostingRegressor 

class ML_model:

    def __init__(self, logger , df, path):
        """
        This is a init function 
        """
        self.df = df
        self.path = path
        self.logger = logger
        self.accuracy = []
        self.random_state = 101
        self.model = [LinearRegression() , SVR() , DecisionTreeRegressor()]
        self.ensemble = [BaggingRegressor(),RandomForestRegressor(), AdaBoostRegressor(), 
                        GradientBoostingRegressor(), XGBRegressor()]
        self.logger.log("Check","ML training Initialized")

    def separating_df_into_x_y(self, df):
        
        """
        This function will separate the dataset into X and y dataframe

        Input df i.e Dataframe
        Output X and y i.e Dataframe
        """
        try:
            self.logger.log("Check","Separating data into X and y Initialized")
            self.X = df.drop(["Cluster","SalePrice"], axis = 1)
            self.y = df["SalePrice"]
            self.x_train, self.x_test , self.y_train, self.y_test = train_test_split(self.X, self.y, test_size = 0.25 , random_state = self.random_state)
            self.logger.log("Done ","Separating data into X and y Completed")

        except Exception as e:
            self.logger.log("Error",e)

    def model_selection(self):

        """
        This function will select model

        Input: N/A
        Output: model index i.e int
        """

        try:
            self.logger.log("Check","Model Selection Initialized")
            accuracy = []
            for i in self.model:
                model = i
                model.fit(self.x_train, self.y_train)
                accuracy.append(model.score(self.x_test, self.y_test))

            for i in range(len(accuracy)):
                if(accuracy[i] == max(accuracy)):
                    self.logger.log("Done ","Model Selection Completed")
                    return i

        except Exception as e:
            self.logger.log("Error",e)
        
    def get_max_model(self):

        """
        This function will find the best possible parameters of our selected model

        Input: model index
        OutPut: tunning model 
        """
        
        try:
            self.logger.log("Check", "Selecting best model Initialized")
            self.separating_df_into_x_y(self.df)
            model = self.model_selection()
            self.decison_tree = False
    
            if(model == 0):
                self.trail_model = LinearRegression()
                self.parameters = {
                                    }
            if(model == 1):

                self.trail_model = SVR()
                self.parameters = {
                                "kernel":["linear","poly","rbf","sigmoid"],
                                "gamma":["scale", "auto"],
                                }
        
            if(model == 2):
            
                self.trail_model = DecisionTreeRegressor()
                self.parameters = {
                                "criterion": ["mse", "friedman_mse", "mae"],
                                "splitter": ["best", "random"]
                                }
                self.decison_tree = True
            
            self.gs = GridSearchCV(estimator = self.trail_model, param_grid= self.parameters, verbose = 3)
            self.gs.fit(self.X, self.y)
            self.logger.log("Done ","Selecting best model Completed")
        
        except Exception as e:
            self.logger.log("Error", e)
        
    def ensemble_selection_and_exporting(self,k):

        """
        More tunning by adding ensemble approach to model and selecting the best possible model
        
        Input : previous model i.e trail model
        Output: Final model and that to saving in a directory
        """

        try:  
            self.logger.log("Check","Ensemble selection and exporting Initialized") 
            accuracy = []
            self.final_model = []


            final_model = self.trail_model
            final_model = self.trail_model.set_params(**self.gs.best_params_)
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))
            
            final_model = BaggingRegressor(base_estimator = final_model)
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))

            final_model = AdaBoostRegressor(base_estimator = final_model)
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))

            final_model = GradientBoostingRegressor()
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))
        
            final_model = XGBRegressor(base_estimator = final_model)
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))

            final_model= self.trail_model
            final_model.fit(self.x_train, self.y_train)
            accuracy.append(final_model.score(self.x_test, self.y_test))


            if(self.decison_tree):
                final_model = RandomForestRegressor()
                final_model.fit(self.x_train, self.y_train)
                accuracy.append(final_model.score(self.x_test, self.y_test))

            for i in range(len(accuracy)):
                if(accuracy[i] == max(accuracy)):
                    model_no = i


            if(model_no == 0):
                final_model = final_model
                final_model.fit(self.X, self.y)
                
            if(model_no == 1):
                final_model = BaggingRegressor(base_estimator = final_model)
                final_model.fit(self.X, self.y)

            if(model_no == 2):
                final_model = AdaBoostRegressor(base_estimator = final_model)
                final_model.fit(self.X, self.y)

            if(model_no == 3):
                final_model = GradientBoostingRegressor()
                final_model.fit(self.X, self.y)

            if(model_no == 4):
                final_model = XGBRegressor(base_estimator = final_model)
                final_model.fit(self.X, self.y)

            if(model_no == 5):
                final_model = self.trail_model
                final_model.fit(self.X, self.y)         
        
            if(model_no == 6):
                final_model = RandomForestRegressor()
                final_model.fit(self.X, self.y)

            pickle.dump(final_model, open(self.path  + "/Model_files/Model_"+str(k) + "_cluster.pickle" , "wb"))
            self.logger.log("Check",str(final_model) + " model is selected for cluster " + str(k) )
            acc = final_model.score(self.x_test , self.y_test)
            self.logger.log("Check",str(acc)+" is the accuracy")
            self.logger.log("Done ","Ensemble selection and exporting Completed")

        
        except Exception as e:
            self.logger.log("Error", e)

    def ML_training_pipeline(self,k):
        """
        This function is used to create a pipeline of all above functions

        Input : N/A
        Output : N/A
        """
        try:
            self.logger.log("Check","ML Training Pipeline Started")
            self.get_max_model()
            final_model = self.ensemble_selection_and_exporting(k)
            self.logger.log("Done ","ML Training Pipeline Completed")
        except Exception as e:
            self.logger.log("Error", e)