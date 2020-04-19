#Neural Network scatch model
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import torch
from torch import nn
import pymysql
import csv
import ast
import pandas as pd
import time
host_name = "localhost"
username = "sugyeong"
password = "12341234"
database_name = "kt_db"

db = pymysql.connect(
    host=host_name,  # DATABASE_HOST
    port=3306,
    user=username,  # DATABASE_USERNAME
    passwd=password,  # DATABASE_PASSWORD
    db=database_name,  # DATABASE_NAME
    charset='utf8'
)
cursor = db.cursor()

sql = """
set session 
sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
"""
cursor.execute(sql)
db.commit()


##################
# Data load
##################
# - 시간 별 데이터 셋 넣어보는 것도 좋을 듯

start = time.time()
sql = """
select d.id,d.timezn_cd, c.*, d.total
from kt as d, (SELECT a.etl_ymd, a.weekday, a.holiday, b.max_temp, b.min_temp, b.pm10_max  
FROM etl_ymd as a, etl_ymd_weather as b  
where a.etl_ymd = b.etl_ymd
and a.etl_ymd like '2018010%') as c
where c.etl_ymd = d.etl_ymd  ;
"""
df = pd.read_sql(sql, db)
print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
print(df)


# 데이터 타입을 float로 바꾸기!
df=df.astype({'pm10_max':'float64', 'total':'float64', 'id':'float64', 'timezn_cd':'float64', 'holiday':'float64', 'weekday':'float64', 'etl_ymd':'float64'})
df.dtypes
data = df

##################
# 데이터 나누기
##################
test_data = data[-21*24:]
target_fields = ['total']
#['id', 'timezn_cd', 'etl_ymd', 'weekday', 'holiday', 'max_temp', 'min_temp', 'pm10_max']
features, targets = data.drop(target_fields, axis=1), data[target_fields]

test_features, test_targets = test_data.drop(target_fields, axis=1), test_data[target_fields]
train_features, train_targets = features[:-60*24], targets[:-60*24]
val_features, val_targets = features[-60*24:], targets[-60*24:]


################
# 모델 만들기
################

import torch
from torch import nn
import matplotlib.pyplot as plt

y = torch.from_numpy(train_targets.values).float()
X = torch.from_numpy(train_features.values).float()
xPredicted = torch.from_numpy(train_features.values).float()[1]# 1 X 2 tensor
print(X.size()[1])
print(y.size())



class Neural_Network(nn.Module):
    def __init__(self, ):
        super(Neural_Network, self).__init__()
        # parameters
        # TODO: parameters can be parameterized instead of declaring them here
        self.inputSize =  X.size()[1]
        self.outputSize = 1
        self.hiddenSize = 20
        
        # weights
        self.W1 = torch.randn(self.inputSize, self.hiddenSize) # 2 X 3 tensor
        self.W2 = torch.randn(self.hiddenSize, self.outputSize) # 3 X 1 tensor
        
    def forward(self, X):
        self.z = torch.matmul(X, self.W1) # 3 X 3 ".dot" does not broadcast in PyTorch
        self.z2 = self.sigmoid(self.z) # activation function
        self.z3 = torch.matmul(self.z2, self.W2)
        o = self.sigmoid(self.z3) # final activation function
        return o
        
    def sigmoid(self, s):
        return 1 / (1 + torch.exp(-s))
    
    def sigmoidPrime(self, s):
        # derivative of sigmoid
        return s * (1 - s)
    
    def backward(self, X, y, o):
        self.o_error = y - o # error in output
        self.o_delta = self.o_error * self.sigmoidPrime(o) # derivative of sig to error
        self.z2_error = torch.matmul(self.o_delta, torch.t(self.W2))
        self.z2_delta = self.z2_error * self.sigmoidPrime(self.z2)
        self.W1 += torch.matmul(torch.t(X), self.z2_delta)
        self.W2 += torch.matmul(torch.t(self.z2), self.o_delta)
        
    def train(self, X, y):
        # forward + backward pass for training
        o = self.forward(X)
        self.backward(X, y, o)
        
    def saveWeights(self, model):
        # we will use the PyTorch internal storage functions
        torch.save(model, "NN")
        # you can reload model with all the weights and so forth with:
        # torch.load("NN")
        
    def predict(self):
        print ("Predicted data based on trained weights: ")
        print ("Input (scaled): \n" + str(xPredicted))
        print ("Output: \n" + str(self.forward(xPredicted)))



NN = Neural_Network()
for i in range(30):  # trains the NN 1,000 times
    print ("#" + str(i).zfill(2) + " Loss: " + str(torch.mean((y - NN(X))**2).detach().item()))  # mean sum squared loss
    NN.train(X, y)
NN.saveWeights(NN)
NN.predict()
