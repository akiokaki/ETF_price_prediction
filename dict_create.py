import os
import csv
import statistics as stats
import numpy as np
import pandas as pd
import time
import datetime
import multiprocessing

# Columns:
# Date,Open,High,Low,Close,Volume,OpenInt
# 2008-03-28,43.64,43.64,43.64,43.64,231,0

def pandas_csv_output():
    '''
    combines the txt files and outputs as csv
    '''

    content_dict = {}
    temp = []
    # txt_search_list = ['aadr.us.txt', 'aaxj.us.txt'] #DEBUG
    txt_search_list = os.listdir('./dataset/ETFs')
    if len(txt_search_list) > 0:
        index = 0
        for each_txt in txt_search_list:
            path = 'dataset/ETFs/'+each_txt
            # with open(path, 'r') as f:
            #     content_string_with_header = str(f.read())
            dict = csv.DictReader(open(path))
            for i in dict:
                #Date,Open,High,Low,Close,Volume,OpenInt
                content_dict[index] = {
                    'etf': each_txt.split('.')[0],
                    'Date': i['Date'],
                    'Open': i['Open'],
                    'High': i['High'],
                    'Low': i['Low'],
                    'Close': i['Close'],
                    'Volume': i['Volume'],
                    'OpenInt': i['OpenInt']
                }
                index+=1
        df = pd.DataFrame.from_dict(
            content_dict,
            orient = 'index',
            columns = ['etf', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInt'])
        # print(df.head(3))\
    else: print('listdir didn\'t output anything')
    data.to_csv('data.csv')

    return
# pandas_csv_output()


# In[3]:


def pandas_output():
    '''
    read csv into pandas df
    '''

    print('reading data')
    s = time.time()
    df = pd.read_csv('data.csv')
    print("time to load: {} secs".format(time.time()-s))
    return df


# In[4]:


def datetime_converter(item):
    '''
    converts an item inside a pd.Series into datetime obj
    '''
    itemized_date = item.split('-')
    yr = itemized_date[0]
    mth = itemized_date[1]
    day = itemized_date[2]

    return datetime.date(int(yr),int(mth),int(day))


def autocorrelation_measures(series_slice_int, fn):
    '''
    calculate autocorrelations for variable on
    each ETFs AND for 1-30 day lags

    returns a dictionary:
        KEY = ETF_name
        VALUE = {lag_time: autocorrelation_value}
    '''

    data = pandas_output()
    data = data.drop(axis=1, columns=['Unnamed: 0'])
    data['ratio'] = data['Close']/data['Volume']
    data['difference_OC'] = data['Open'] - data['Close']
    data = data.drop(data.index[[736057, 1988441]])
    # data = data.drop(data.index[[1988441]])
    data['Date'] = data['Date'].apply(datetime_converter)

    if series_slice_int == 0:
        series_slice = data['etf'].unique()[1:2]
    elif series_slice_int == 1:
        series_slice = data['etf'].unique()[0:150]
    elif series_slice_int == 2:
        series_slice = data['etf'].unique()[150:300]
    elif series_slice_int == 3:
        series_slice = data['etf'].unique()[300:450]
    elif series_slice_int == 4:
        series_slice = data['etf'].unique()[450:600]
    elif series_slice_int == 5:
        series_slice = data['etf'].unique()[600:850]
    elif series_slice_int == 6:
        series_slice = data['etf'].unique()[850:1000]
    elif series_slice_int == 7:
        series_slice = data['etf'].unique()[1000:1250]
    elif series_slice_int == 8:
        series_slice = data['etf'].unique()[1250:]

    lag_dicts = {}
    for each_etf in series_slice:
        s = time.time()
        vals = {}
        for lag_time in range(1,31):
            lag_val = data[data['etf']== each_etf]['Close'].autocorr(lag=lag_time)
            if np.isnan(lag_val):
                pass
            else:
                vals[lag_time] = lag_val
        lag_dicts[each_etf] = vals
        print("{}, {}sec".format(each_etf, (time.time()-s)))
    with open('{}.txt'.format(fn), 'w') as f:
        f.write(str(lag_dicts))
    return


if __name__ == "__main__":
    ## Read in data and clean up
    # data = pandas_output()
    # data = data.drop(axis=1, columns=['Unnamed: 0'])
    # data['ratio'] = data['Close']/data['Volume']
    # data['difference_OC'] = data['Open'] - data['Close']
    # data = data.drop(data.index[[736057, 1988441]])
    # # data = data.drop(data.index[[1988441]])
    # data['Date'] = data['Date'].apply(datetime_converter)

	# Create a list of jobs and then iterate through
	# the number of processes appending each process to
	# the job list
    dictionary_creation = True
    if dictionary_creation:
        jobs = []
        process = multiprocessing.Process(target=autocorrelation_measures, args=(1, "output1"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(2, "output2"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(3, "output3"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(4, "output4"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(5, "output5"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(6, "output6"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(7, "output7"))
        jobs.append(process)
        process = multiprocessing.Process(target=autocorrelation_measures, args=(8, "output8"))
        jobs.append(process)
    	# Start the processes (i.e. calculate the random number lists)
        for j in jobs:
            j.start()
    	# Ensure all of the processes have finished
        for j in jobs:
            j.join()
        print("List processing complete.")
