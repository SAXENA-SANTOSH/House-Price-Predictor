from flask import Flask , render_template , request , jsonify , url_for
import pickle
import numpy as np 
import os
from Training import Training
from Logger.Activity_logger import Activity_logger
import shutil
from prediction import Single_Prediction , Multiple_Prediction


#path = "/Users/santoshsaxena/Desktop/wafer"
path = ""

app = Flask(__name__)

@app.route("/" , methods = ["GET" , "POST"])
def home_page():
    try:
        return render_template("Homepage.html")
    except:
        return render_template("Error.html")

@app.route("/training", methods = ["GET"] )
def training():
    try:
        logger = Activity_logger(path)
        obj = Training()
        obj.training_pipeline(logger, path)
        return render_template("Done.html")
    except:
        return render_template("Error.html")
    
@app.route("/single", methods = ["GET"])
def single():
    try:
        return render_template("InputPage.html")
    except:
        return render_template("Error.html")

@app.route("/logs" , methods = ["GET"] )
def log():
    try:
        file = open(path + '/Logs/log.txt', 'r')
        return render_template("Logs.html", text = file.read())
    except:
        return render_template("Error.html")

@app.route("/back", methods = ["GET", "POST"])
def back():
    try:
        return render_template("Homepage.html")
    except:
        return render_template("Error.html")

@app.route("/multiple", methods = ["GET"])
def multiple():
    try:
        return render_template("Multiple_prediction.html")
    except:
        return render_template("Error.html")
@app.route("/plots", methods = ["GET"])
def plot():
    try:
        if(os.path.isfile(path + "Model_file/Dimensions.jpg")):
            os.remove(path + "Model_file/Dimensions.jpg")
        if(os.path.isfile(path + "Model_file/Cluster.jpg")):
            os.remove(path + "Model_file/Cluster.jpg")
        shutil.copy(path + "/Model_files/Plots/Cluster.jpg" , path + "/static/Cluster1.jpg")
        shutil.copy(path + "/Model_files/Plots/Dimensions.jpg" , path + "/static/Dimensions1.jpg")
        return render_template("Plots.html")
    except:
        return render_template("Error.html")

@app.route("/reset" , methods = ["GET"])
def reset():
    try:
        return render_template("InputPage.html")
    except:
        return render_template("Error.html")

@app.route("/multiple_predict" , methods= ["POST"])
def multiple_prediction():
    try:
        if(request.method == "POST"):
            target_path = request.form["path"]
        pr = Multiple_Prediction("/Users/santoshsaxena/Desktop/wafer", target_path)
        data = pr.multiple_predictions()
        return data.to_html()
    except:
        return render_template("Error.html")




if __name__ == "__main__":
    app.run(debug=True)