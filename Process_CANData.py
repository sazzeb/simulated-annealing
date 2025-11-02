# Asynchronous data processing and reading of the CAN Bus data


import multiprocessing
import numpy as np
import can
import time
import threading
import sys
import struct
import queue
import pandas as pd
from math import *
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report,f1_score,roc_auc_score,precision_score, recall_score

interface = "vcan0"
send_queue = queue.Queue()
exitapp = False


def Handle_CAN_Data():
    try:
        messages = send_queue.get()
        process_can_data(messages)

    except OSError:
        sys.exit()


def machine_learning_model(df):

    X = df.values

    loaded_rf = joblib.load("random_forest.joblib")

    try:
        y_pred = loaded_rf.predict(X)
    except:
        y_pred = []

    return None


def get_ID(can_frames):
    canData = []
    speed = []
    rpm = []
    
    for row in can_frames:
        row = str(row)
        record = {'PID': row[41:44], 'Value' : int((row[96:98] +  row[99:101]),16)}
        if record['PID'] == '254':
            record['Value'] = (record['Value'] * 0.62137119) / 100
            speed.append(record['Value'])


        if record['PID'] == '115':
            record['Value'] = record['Value'] * 2
            rpm.append(record['Value'])

        canData.append(record)
        machine_learning_model(dict_to_df(canData))
        record = {}

    return None


def dict_to_df(dict):

  #load dictionary to dataframe

  df = pd.DataFrame.from_dict(dict)

  df = df.loc[(df['PID'] == '115') | (df['PID'] == '254')]
  
  df = df.reset_index(drop=True)
  
  one_hot = pd.get_dummies(df['PID'])
  
  df = df.drop('PID',axis = 1)
  
  df = df.join(one_hot)

  df.rename(columns = {'115':'RPM', '254':'Speed'}, inplace = True)

  try:
    df.loc[df['RPM'] == 1, 'RPM'] = df['Value']

  except:
    df['RPM'] = 0

  try:
    df.loc[df['Speed'] == 1, 'Speed'] = df['Value']
  
  except:
    df['Speed'] = 0

  df = df.drop(columns=['Value'])
  return df


def process_can_data(can_data):
    window = 100
    message_ID = get_ID(can_data)

def listen():


    """Wait on a message from a socket OR a shutdown signal."""
    print("listen() starting")

    # Create an can socket, STREAMing socket
    try:
        bus = can.interface.Bus(bustype='socketcan', channel=interface)
    except OSError:
        sys.stderr.write(f'cannot not bind to interface, {interface}')
    if not bus:
        print("error connecting to the bus")

    # Bind the socket to the server
    batch = 1
    
    batch_file = open('batch_process_time.txt','w')
    while True:


        
        can_msg = []
        msg = []
        # Receiving the messages out of the bus direclty
        try:
            i = 1
            while i < 1000:
                can_msg.append(bus.recv())
                i += 1

            send_queue.put(can_msg)
            start = time.time()
            Handle_CAN_Data()
            end = time.time()
            print("The time it took to receive and execute Batch number " + str(batch) +  " was " + str(end-start))

            batch_file.write("The time it took to receive and execute Batch number " + str(batch) +  " was " + str(end-start) + "\n")

            can_msg = []
        except OSError:
            sys.stderr.write("Can't read data from the bus")

        batch += 1
    batch_file.close()
    sys.exit("Done!")

if __name__ == "__main__":

    try:
        listen()
        process = mp.Process(target=Handle_CAN_Data, args=())
        process.daemon = True
        process.start()

    except (KeyboardInterrupt, SystemExit):
        sys.exit()
        process.terminate()
        process.join()
    if KeyboardInterrupt:
        sys.exit()
        process.terminate()
        process.join()
