%pip install xgboost
%pip install mlflow

import pandas as pd
import xgboost as xgb
import mlflow
import mlflow.xgboost

raw_input = 
pd.read_csv("/dbfs/databricks-datasets/Rdatasets/data-001/csv/datasets/iris.csv",
                        header = 0,
                       names=["item","sepal length","sepal width", "petal 
length", "petal width","class"])
new_input = raw_input.drop(columns=["item"])
new_input["class"] = new_input["class"].astype('category')
new_input["classIndex"] = new_input["class"].cat.codes
print(new_input)
print('finished')
