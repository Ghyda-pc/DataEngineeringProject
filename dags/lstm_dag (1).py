
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
import pandas as pd


def create_rnn_dataset(data, lookback=1):
    import numpy as np
    data_x, data_y = [], []
    for i in range(len(data)- lookback -1):
              #All points from this point, looking backwards upto lookback
              a = data[i:(i+ lookback), 0]
              data_x.append(a)
              #The next point
              data_y.append(data[i + lookback, 0])
    return np.array(data_x), np.array(data_y)


def Extract():
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine
    import matplotlib.pyplot as plt
    host="postgres"
    database="airflow"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    cases=pd.read_sql('SELECT * FROM public."Covid19_Jordan_Vaccination_Cases_Scaled"' , engine)
    scaled_cases = cases[['new_cases']]
    np.random.seed(1)

    cases = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv',header=0)
    cases = cases[(cases.location == 'Jordan')]
    cases = cases[['new_cases']]
    from sklearn.preprocessing import StandardScaler

    #Scale the data
    scaler = StandardScaler()
    scaled_cases=scaler.fit_transform(cases)

    train_size = 1 * 30 * 13
    lookback=2 * 30
    train_cases = scaled_cases[0:train_size,:]
    test_cases = scaled_cases[train_size-lookback:,:]
    np.save('/opt/airflow/data/train_cases.npy',train_cases)
    np.save('/opt/airflow/data/test_cases.npy',test_cases)


def Train_Test():
    
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from tensorflow.keras.models import Sequential 
    from tensorflow.keras.layers import LSTM,Dense 
    import tensorflow as tf

    train_cases = np.load('/opt/airflow/data/train_cases.npy')
    test_cases = np.load('/opt/airflow/data/test_cases.npy') 

    lookback=2 * 30
    
    #Create X and Y for training
    train_req_x, train_req_y = create_rnn_dataset(train_cases,lookback)
    #Reshape for use with LSTM
    train_req_x = np.reshape(train_req_x, (train_req_x.shape[0],1, train_req_x.shape[1]))

    tf.random.set_seed(3)

    #Create a Keras Model
    ts_model=Sequential()

    #Add LSTM
    ts_model.add(LSTM(256, input_shape=(1,lookback)))
    ts_model.add(Dense(1))

    #Compile with Adam Optimizer. Optimize for minimum mean square error
    ts_model.compile(loss="mean_squared_error", optimizer="adam", metrics=["mse"])

    #Train the model
    ts_model.fit(train_req_x, train_req_y, epochs=5, batch_size=1, verbose=1)


    #Preprocess the test dataset, the same way training set is processed
    test_req_x, test_req_y = create_rnn_dataset(test_cases,lookback)
    test_req_x = np.reshape(test_req_x, (test_req_x.shape[0],1, test_req_x.shape[1]))
    #Evaluate the model
    ts_model.evaluate(test_req_x, test_req_y, verbose=1)
    #Predict for the training dataset
    predict_on_train= ts_model.predict(train_req_x)
    #Predict on the test dataset
    predict_on_test = ts_model.predict(test_req_x)
    cases = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv',header=0)
    cases = cases[(cases.location == 'Jordan')]
    cases = cases[['new_cases']]
    from sklearn.preprocessing import StandardScaler

    #Scale the data
    scaler = StandardScaler()
    scaled_cases= scaler.fit_transform(cases)
    
    #Inverse the scaling to view results
    predict_on_train = scaler.inverse_transform(predict_on_train)
    predict_on_test = scaler.inverse_transform(predict_on_test)


    #Use last part of the training data as the initial lookback
    curr_input= test_req_x[-1,:].flatten()

    #Predict for the next three months
    predict_for = 3 * 30

    for i in range(predict_for):
        
        #Take the last lookback no. of samples as X
        this_input=curr_input[-lookback:]
        #Create the input
        this_input=this_input.reshape((1,1,lookback))
        #Predict for the next point
        this_prediction=ts_model.predict(this_input)
        #Add the current prediction to the input
        curr_input = np.append(curr_input,this_prediction.flatten())

    #Extract the last predict_for part of curr_input, which contains all the new predictions
    predict_on_future=np.reshape(np.array(curr_input[-predict_for:]),(predict_for,1))
    # #Inverse to view results

    predict_on_future=scaler.inverse_transform(predict_on_future)
    np.save('/opt/airflow/output/predict_on_train.npy',predict_on_train)
    np.save('/opt/airflow/output/predict_on_test.npy',predict_on_test)
    np.save('/opt/airflow/output/predict_on_future.npy',predict_on_future)
    #predict_on_train.close
    
def Plot():
    import numpy as np 
    import matplotlib.pyplot as plt
    predict_on_train = np.load('/opt/airflow/output/predict_on_train.npy')
    predict_on_test = np.load('/opt/airflow/output/predict_on_test.npy')
    predict_on_future = np.load('/opt/airflow/output/predict_on_future.npy')
    #Plot the training data with the forecast data
    total_size = len(predict_on_train) + len(predict_on_test) + len(predict_on_future)
    #Setup training chart
    predict_train_plot = np.empty((total_size,1))
    predict_train_plot[:, :] = np.nan
    predict_train_plot[0:len(predict_on_train), :] = predict_on_train
    #Setup test chart
    predict_test_plot = np.empty((total_size,1))
    predict_test_plot[:, :] = np.nan
    predict_test_plot[len(predict_on_train):len(predict_on_train)+len(predict_on_test), :] = predict_on_test
    #Setup future forecast chart
    predict_future_plot = np.empty((total_size,1))
    predict_future_plot[:, :] = np.nan
    predict_future_plot[len(predict_on_train)+len(predict_on_test):total_size, :] = predict_on_future
    plt.figure(figsize=(20,10)).suptitle("Plot Predictions for Training, Test & Forecast Data", fontsize=20)
    plt.plot(predict_train_plot)
    plt.plot(predict_test_plot)
    plt.plot(predict_future_plot)
    plt.savefig('/opt/airflow/output/plot_train_test_forcast.png')


  
with DAG("LSTM_DAG", start_date=datetime(2021, 5, 26),
    schedule_interval="@daily", catchup=False) as dag:
    
        Extract1 =PythonOperator(
            task_id="Extract_Data_From_POSTGRES",
            python_callable=Extract
        )
        Plotting =PythonOperator(
            task_id="Plot",
            python_callable= Plot 
        )
        Train_Test1 =PythonOperator(
            task_id="Train_Test_Forcast",
            python_callable= Train_Test 
        )
        


        Install_dependecies = BashOperator(task_id='installing',bash_command='pip install sklearn matplotlib')
 
        Install_dependecies1 = BashOperator(task_id='installing1',bash_command='pip install tensorflow')

Install_dependecies >> Install_dependecies1 >> Extract1 >> Train_Test1 >> Plotting 