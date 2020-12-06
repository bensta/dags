import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tempfile
from sklearn import linear_model
from sklearn.metrics import r2_score


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
import logging
import os

def train_model():
    pg_hook = PostgresHook()
    data = pd.read_sql_query('SELECT "ENGINESIZE", "CO2EMISSIONS" FROM public.vehicle_fuel_consumptions',con=pg_hook.get_conn())
    
    train = data[:(int((len(data)*0.8)))]
    test = data[(int((len(data)*0.8))):]# Modeling:

    # Using sklearn package to model data :
    regr = linear_model.LinearRegression()
    train_x = np.array(train[["ENGINESIZE"]])
    train_y = np.array(train[["CO2EMISSIONS"]])
    regr.fit(train_x,train_y)# The coefficients:


    my_engine_size = 3.5
    estimatd_emission = get_regression_predictions(my_engine_size,regr.intercept_[0],regr.coef_[0][0])
    logging.info ("Estimated Emission {}:".format(estimatd_emission))# Checking various accuracy:

    test_x = np.array(test[["ENGINESIZE"]])
    test_y = np.array(test[["CO2EMISSIONS"]])
    test_y_ = regr.predict(test_x)
    logging.info("Mean absolute error: {}".format(np.mean(np.absolute(test_y_ - test_y))))
    logging.info("Mean sum of squares (MSE): {}".format(np.mean((test_y_ - test_y) ** 2)))
    logging.info("R2-score: {}".format(r2_score(test_y_ , test_y)))

    hook = GoogleCloudStorageHook()
    with tempfile.NamedTemporaryFile() as fp:
        pickle.dump(regr, fp)
        fp.seek(0)
        hook.upload("ai-demo-bucket.airlaunch.ch", "ai/models/model.pkl", fp.name)

# Function for predicting future values :
def get_regression_predictions(input_features,intercept,slope):
    predicted_values = input_features*slope + intercept
    return predicted_values# Predicting emission for future car:

with DAG('training_pipeline', start_date=datetime(2016, 1, 1)) as dag:
    t1 = PythonOperator( 
        task_id='train',
        python_callable=train_model,
        dag=dag)
