"""
This Class will preprocess the data. The Preprocessing steps used in the model are -

1) Dimensionality reduction ----------> for no Multi-Collinearity
2) Clustering data -------------------> Data is segregated into clusters 
Different models will be trained for different cluster for better accuracy

Written by : Santosh Saxena
on 14/2/2021

"""

import os
import kneed
import shutil
import pickle
import matplotlib
matplotlib.use('Agg')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from statsmodels.stats.outliers_influence import variance_inflation_factor


class Data_preprocessing:

    def __init__(self, logger , path): 

        self.path = path
        self.logger = logger
        self.df = pd.read_csv(self.path + "/Input_files/Processed_input.csv")
        self.logger.log("Check", "Data Preprocessing Initialized")

    def generating_files(self):

        """
        This function will generate files used further to store files.
        """

        try:
            self.logger.log("Check","Generating_Files Initialized")
            
            if(os.path.isdir(self.path + "/Model_files/Plots")):
                shutil.rmtree(self.path+"/Model_files/Plots")
            os.mkdir(self.path + "/Model_files/Plots")

            if(os.path.isdir(self.path + "/Input_files/Cluster_files")):
                shutil.rmtree(self.path + "/Input_files/Cluster_files")
            os.mkdir(self.path+"/Input_files/Cluster_files")

            if(os.path.isdir(self.path + "/Model_files/Models")):
                shutil.rmtree(self.path+"/Model_files/Models")
            os.mkdir(self.path + "/Model_files/Models")

            self.logger.log("Done ","Generating_Files Completed")


        except Exception as e:
            self.logger.log("Error",e)

    def multi_collinearity(self):
        x = self.df.drop(["SalePrice"], axis = 1)
        vif = []
        features = list(x.columns)
        print("\n\nStart\n\n")
        print(self.df.columns[self.df.isnull().sum() != 0])
        for i in range(len(x.columns)):
            vif.append(variance_inflation_factor(np.array(x),i))
        print("\n\nThis is done\n\n")
        VIF = pd.DataFrame({"Features":features,"VIF":vif})
        VIF.to_csv(self.path+"/Model_files/VIF.csv", index = False)
        x["Derived_feature"] = x[list(VIF[VIF["VIF"] > 7]["Features"])].mean(axis = 1)
        x.drop(x[list(VIF[VIF["VIF"] > 7]["Features"])], axis = 1, inplace = True)
        self.df = x.join(self.df["SalePrice"])

    def dimensionality_reduction(self):

        """
        This function will reduce the dimensions with the help of PCA
        """

        try:
            self.logger.log("Check","Dimensionality Redction Initialized")
            pca = PCA()
            temp = pca.fit_transform(self.df.drop(["SalePrice"], axis = 1))

            no_of_features = len(self.df.columns) - 1
            information = np.cumsum(pca.explained_variance_ratio_)

            number_of_dimensions = (information < 0.96).sum()
        
            plt.style.use("classic")
            plt.figure()
            plt.plot(range(0, no_of_features) , information ,color = "blue", label = "information vs dimensions")
            plt.plot([number_of_dimensions, number_of_dimensions] ,[0,1] ,color = "black" ,label = "no_of_dimensions_used")
            plt.xlabel("Number of Dimensions")
            plt.ylabel("Information of Data")
            plt.legend(loc="lower left")
            plt.savefig(self.path + "/Model_files/Plots/Dimensions.jpg")

            pca = PCA(n_components=number_of_dimensions)
            x = self.df.drop(["SalePrice"], axis = 1)
            y = self.df[["SalePrice"]]
            self.df = pca.fit_transform(x)
            self.df = pd.DataFrame(self.df)
            self.df["SalePrice"] = y
            self.df.to_csv(self.path +"/Input_files/Dimensionally_reduced_input.csv", index = False)
            pickle.dump(pca, open(self.path+"/Model_files/Models/PCA.pickle","wb"))
            self.logger.log("Done ","Dimensionality Reduction Completed")

        except Exception as e:
            self.logger.log("Error",e)

    def clustering_data(self):

        """
        This function will segregate the data into cluster
        """

        try:
            self.logger.log("Check","Clustering Initialized")
            self.wcss = []
            x = self.df.drop(["SalePrice"], axis = 1)
            for i in range(1,30):
                model = KMeans(n_clusters= i)
                model.fit(x)
                self.wcss.append(model.inertia_)

            k = kneed.KneeLocator(range(1,30),self.wcss,curve="convex", direction="decreasing")
            self.knee = k.knee
            self.wcss = np.array(self.wcss)/max(self.wcss)
            plt.figure()
            plt.plot(range(1,30), self.wcss , label = "WCSS vs Inertia", color = "blue")
            plt.plot([self.knee,self.knee],[min(self.wcss),1], label = "Number of cluster used", color = "black")
            plt.xlabel("No of cluster")
            plt.ylabel("WCSS")
            plt.legend(loc = "upper right")
            plt.savefig(self.path + "/Model_files/Plots/Cluster.jpg")
            self.logger.log("Done ","Clustering Completed")

        except Exception as e:
            self.logger.log("Error", e)
        
        
    def expoting_clusters(self):

        """
        In this function , Each cluster data will be separated and stored 
        """
        try:
            self.logger.log("Check", "Exporting Clusered dataset Initialized")
            model = KMeans(n_clusters= self.knee)
            x = self.df.drop(["SalePrice"], axis = 1)
            print(x.columns)
            print(np.shape(x))
            model.fit(x)
            cluster_no = model.predict(x)
            self.df["Cluster"] = cluster_no

            pickle.dump(model,open(self.path+"/Model_files/Models/Cluster.pickle","wb"))

            for i in range(1,self.knee):
                self.df[self.df["Cluster"] == i].to_csv(self.path + "/Input_files/Cluster_files/"+str(i)+"_cluster.csv", index = False)
            self.logger.log("Done ","Exporting Clustered dataset Completed")
            

        except Exception as e:
            self.logger.log("Error",e)
            
    def data_preprocessing_pipeline(self):
        
        """
        All the class functions are called accordingly to form a pipeline
        """

        try:
            self.logger.log("Check", "Data Preprocessing Pipeline Initialized")
            self.generating_files()
            self.multi_collinearity()
            self.dimensionality_reduction()
            self.clustering_data()
            self.expoting_clusters()
            self.logger.log("Done ", "Data Preprocessing Pipeline Completed")
        except Exception as e:
            self.logger.log("Error",e)
