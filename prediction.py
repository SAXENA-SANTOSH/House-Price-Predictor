import os
import pickle
import shutil
import numpy as np
import pandas as pd 


class Single_Prediction:

    def __init__(self,path , test):
        self.test_data = test
        self.path = path

    def single_prediction(self):
        self.d_type = pd.read_csv(self.path + "/Source of Truth/Data_type.csv")
        self.cluster = pickle.load(open(self.path + "/Model_files/Models/Cluster.pickle", "rb"))
        self.pca = pickle.load(open(self.path + "/Model_files/Models/PCA.pickle", "rb"))
        self.sc_x = pickle.load(open(self.path + "/Model_files/Scaler/scaler_x.pickle","rb"))

        for i in np.where(self.d_type["SOT"] == "object")[0]:
            encoder = pickle.load(open(self.path+"/Model_files/Encoder/"+str(self.d_type.iloc[i]["Unnamed: 0"])+"_encoder.pickle", "rb"))
            self.test_data[i-1] = encoder.transform([self.test_data[i-1]])[0]

        value = []
        index = []
        for i in np.where(self.d_type["SOT"] != "object")[0]:
            if(i != 0 and i != 76):
                value.append(self.test_data[i-1])
                index.append(i-1)
        
        result = self.sc_x.transform([value])[0]
        for i in range(len(index)):
            self.test_data[index[i]] = result[i]

        vif = pd.read_csv(self.path + "/Model_files/VIF.csv")
        collinear = []
        index = []
        for i in np.where(vif["VIF"] > 7)[0]:
            collinear.append(self.test_data[i])
            index.append(i)
            index.sort(reverse=True)
        for i in index:
            del self.test_data[i]

        self.test_data.append(np.mean(collinear))
        
        result = self.pca.transform([self.test_data])

        cluster_no = self.cluster.predict(result)[0]

        self.ML_model = pickle.load(open(self.path + "/Model_files/Model_" +str(cluster_no) + "_cluster.pickle","rb"))
        
        result = round(self.ML_model.predict(result)[0],2)

        return result


class Multiple_Prediction:

    def __init__(self, path , target_path):
        self.path = path
        self.target_path = target_path

    def multiple_predictions(self):
        try:
            self.test_data = pd.read_csv(self.target_path)
            self.d_type = pd.read_csv(self.path + "/Source of Truth/Data_type.csv")
            self.cluster = pickle.load(open(self.path + "/Model_files/Models/Cluster.pickle", "rb"))
            self.pca = pickle.load(open(self.path + "/Model_files/Models/PCA.pickle", "rb"))
            self.sc_x = pickle.load(open(self.path + "/Model_files/Scaler/scaler_x.pickle","rb"))

            if(len(self.test_data.columns[self.test_data.isnull().sum() != 0]) > 0):
                print("NO")
            else:
                x = self.test_data.drop(["Id"], axis = 1)
                x[x.columns[x.dtypes != "object"]] = self.sc_x.transform(x[x.columns[x.dtypes != "object"]])
                for i in os.listdir(self.path + "/Model_files/Encoder/"):
                    encoder = pickle.load(open(self.path + "/Model_files/Encoder/" + str(i) , "rb"))
                    x[i.split("_")[0]] = encoder.transform(x[i.split("_")[0]])
                vif = pd.read_csv(self.path + "/Model_files/VIF.csv")
                x["Derived_feature"] = x[list(vif[vif["VIF"] > 7]["Features"])].mean(axis = 1)
                x.drop(x[list(vif[vif["VIF"] > 7]["Features"])], axis = 1, inplace = True)        
                result = self.pca.transform(x)
                result = pd.DataFrame(result)
                cluster_no = self.cluster.predict(result)
                prediction = []
                for i in range(len(cluster_no)):
                    if(os.path.isfile(self.path + "/Model_files/Model_"+str(cluster_no[i])+"_cluster.pickle")):
                        ML_model = pickle.load(open(self.path + "/Model_files/Model_"+str(cluster_no[i])+"_cluster.pickle", "rb"))
                        prediction.append(round(ML_model.predict(pd.DataFrame(result.iloc[i]).transpose())[0], 2))
                    else:
                        ML_model = pickle.load(open(self.path + "/Model_files/Model_"+str(2)+"_cluster.pickle", "rb"))
                        prediction.append(round(ML_model.predict(pd.DataFrame(result.iloc[i]).transpose())[0], 2))

                self.test_data["SalePrice"] = prediction

                return self.test_data

        except Exception as e:
            print(e)







