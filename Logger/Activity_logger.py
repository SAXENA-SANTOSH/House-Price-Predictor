"""
This class will write logs in log file

Developed by Santosh Saxena
on 5/2/2021

"""


import datetime 

class Activity_logger:
    def __init__(self, path):
        """
        This function will initialize a log file and store it in the respective directory  
        """

        self.path = path
        file = open(path+"/Logs/log.txt","w")
        file.write("  Date       Day       Time        Type          Message\n")
        file.close()

    def log(self , type_ , message): 
        """
        This function will append the log messages to log file.

        There are 3 types of logs
        1) Error log
        2) Check log
        3) Done  log
        """

        self.message = message
        self.type = type_
        file = open(self.path+"/Logs/log.txt","a")
        date = datetime.datetime.now().strftime("%D")
        day = datetime.datetime.now().strftime("%a")
        time = datetime.datetime.now().strftime("%H:%M:%S")
        file.write(str(date)+"     "+str(day)+"     "+str(time)+"     "+str(self.type) +"     "+ str(self.message)+"\n")
        file.close()

