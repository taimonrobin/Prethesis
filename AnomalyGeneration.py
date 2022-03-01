import random
from random import randrange
import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import pandas as pd
import plotly.express as px
from prophet import Prophet
import json
from prophet.serialize import model_to_json, model_from_json


mpl.rcParams['figure.figsize'] = (10, 8)
mpl.rcParams['axes.grid'] = False

dataset_name = "3.4.26"

number_of_anomalies = 100

tolerence_level = 0.75

minimum_instances_of_synthetic_anomaly = 20
maximum_instances_of_synthetic_anomaly = 50

number_of_test_iterations = 100

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def get_list_of_anomalies(df):
    return df.loc[df["anomaly"] == "Yes"]

def boxplot_anomaly(df):
    ft = "source_addr"
    Q1 = df[ft].quantile(0.25)
    Q3 = df[ft].quantile(0.75)
    IQR = Q3 - Q1
    lower_band = Q1 - 1.5 * IQR 
    upper_band = Q3 + 1.5 * IQR
    df['anomaly'] = df[ft].apply(lambda x: 'Yes' if((x < lower_band) | (x > upper_band)) else 'No')
    return df

def zscore_anomaly(df):
    df["zscore"] = (df.source_addr - df.source_addr.mean()) / df.source_addr.std()
    df['anomaly'] = df.zscore.apply(lambda x: 'Yes' if((x > 3) | (x < -3)) else 'No')
    return df

def prophet_anomaly(df_final):
    df_final['datetime']=pd.to_datetime(df_final['datetime'])
    df_final = df_final.reset_index()[['datetime','source_addr']].rename({'datetime':'ds','source_addr':'y'}, axis='columns')
    train = df_final[(df_final['ds'] >= '2019-07-15') & (df_final['ds'] <= '2020-12-14')]
    test = df_final[(df_final['ds'] >= '2020-12-15') & (df_final['ds'] <= '2021-02-23')]
    m = Prophet(interval_width=0.95)
    m.add_country_holidays(country_name='DE')
    m.fit(train)
    future = m.make_future_dataframe(periods=2000,freq='H', include_history = True)
    forecast = m.predict(future)    

    results = pd.concat([df_final.set_index('ds')['y'],forecast.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']]],axis=1)
    results['error'] = results['y'] - results['yhat']
    results["uncertainty"] = results['yhat_upper'] - results['yhat_lower']
    results[results['error'].abs() >  tolerence_level*results['uncertainty']]
    results['anomaly'] = results.apply(lambda x: 'Yes' if(np.abs(x['error']) >  tolerence_level*x['uncertainty']) else 'No', axis=1)
    results = results.reset_index()[['ds','y', 'anomaly']].rename({'ds':'datetime','y':'source_addr'}, axis='columns')

    with open( dataset_name +".json", 'w') as fout:
        json.dump(model_to_json(m), fout)

    return results

def prophet_anomaly_trained(df_final):
    with open(dataset_name +".json", 'r') as fin:
        new_m = model_from_json(json.load(fin))  # Load model
    future = new_m.make_future_dataframe(periods=2000,freq='H',  include_history = True)
    forecast = new_m.predict(future)
    df_final['datetime']=pd.to_datetime(df_final['datetime'])
    df_final = df_final.reset_index()[['datetime','source_addr', 'synthetic_anomaly']].rename({'datetime':'ds','source_addr':'y'}, axis='columns')
    results= pd.concat([df_final.set_index('ds')[['y', 'synthetic_anomaly']],forecast.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']]],axis=1)
    results['error'] = results['y'] - results['yhat']
    results["uncertainty"] = results['yhat_upper'] - results['yhat_lower']
    results[results['error'].abs() >  tolerence_level*results['uncertainty']]
    results['anomaly'] = results.apply(lambda x: 'Yes' if(np.abs(x['error']) >  tolerence_level*x['uncertainty']) else 'No', axis=1)
    results = results.reset_index()[['ds','y', 'anomaly', 'synthetic_anomaly']].rename({'ds':'datetime','y':'source_addr'}, axis='columns')

    return results


def remove_anomaly(df_anomalous_instances, df_final):
    for row in df_anomalous_instances.iterrows():
        loop_condition = True
        anomolous_datetime = datetime.strptime(str(row[1]["datetime"]), '%Y-%m-%d %H:%M:%S')
        hour_counter = 1

        while(loop_condition):
            hours_added = timedelta(hours = hour_counter)
            hour_counter += 1
            normal_datetime = anomolous_datetime + hours_added
            loop_condition = str(normal_datetime) in list(df_anomalous_instances.datetime)
            if(loop_condition == False):
                df_final.loc[df_final["datetime"] == anomolous_datetime.strftime("%Y-%m-%d %H:%M:%S"), "source_addr"] = df_final.loc[df_final["datetime"] == str(normal_datetime)].source_addr.item()
                df_final.loc[df_final["datetime"] == anomolous_datetime.strftime("%Y-%m-%d %H:%M:%S"), "anomaly"] = "No"
    return df_final

def inject_synthetic_anomaly(number_of_anomalies, df_final, d1, d2, minimum_instances_of_synthetic_anomaly, maximum_instances_of_synthetic_anomaly):
    df_final = df_final[["datetime", "source_addr"]]
    df_final["synthetic_anomaly"] = False

    datetime_list = []
    anomalous_instances = []
    for i in range(number_of_anomalies):
        required_date = random_date(d1,d2)
        required_date = required_date.replace(minute=0, second=0)
        datetime_list.append(required_date)
        anomalous_instances.append(random.randint(minimum_instances_of_synthetic_anomaly,maximum_instances_of_synthetic_anomaly))

    for i in range(number_of_anomalies):
        df_final.loc[df_final["datetime"] == datetime_list[i].strftime("%Y-%m-%d %H:%M:%S"), "source_addr"] = anomalous_instances[i]
        df_final.loc[df_final["datetime"] == datetime_list[i].strftime("%Y-%m-%d %H:%M:%S"), "synthetic_anomaly"] = True


    return df_final


df = pd.read_csv("Dataset/source addresses/" + dataset_name +".csv")
df_final = df[(df['datetime'] >= '2019-07-15') & (df['datetime'] <= '2021-02-23')]
d1 = datetime.strptime('7/15/2019 1:00 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('2/23/2021 1:00 AM', '%m/%d/%Y %I:%M %p')


#Remove the data points marked as anomaly by Boxplot
df_final = boxplot_anomaly(df_final)
df_anomalous_instances = get_list_of_anomalies(df_final)
df_final = remove_anomaly(df_anomalous_instances, df_final)

#Remove the data points marked as anomaly by Z Score
df_final = zscore_anomaly(df_final)
df_anomalous_instances = get_list_of_anomalies(df_final)
df_final = remove_anomaly(df_anomalous_instances, df_final)

#Remove the data points marked as anomaly by Prophet
df_final = prophet_anomaly(df_final)
df_anomalous_instances = get_list_of_anomalies(df_final)
df_final = remove_anomaly(df_anomalous_instances, df_final)

total_number_of_synthetic_anomaly = []
list_of_anomalies_found_by_boxplot = []
list_of_anomalies_found_by_zscore = []
list_of_anomalies_found_by_prophet = []
list_of_synthetic_anomalies_found_by_boxplot = []
list_of_synthetic_anomalies_found_by_zscore = []
list_of_synthetic_anomalies_found_by_prophet = []

for i in range(number_of_test_iterations):
    #Inject synthetic anomaly
    df_injected = inject_synthetic_anomaly(number_of_anomalies, df_final, d1, d2, minimum_instances_of_synthetic_anomaly, maximum_instances_of_synthetic_anomaly)

    # #Save the dataframe in csv
    # df_injected.to_csv("Dataset/anomalies injected/" + dataset_name + ".csv", encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

    #Determining the number of synthetic anomaly detected by Boxplot
    df_final_boxplot = boxplot_anomaly(df_injected)
    df_anomalous_instances_boxplot = get_list_of_anomalies(df_final_boxplot)

    #Determining the number of synthetic anomaly detected by Z Score
    df_final_zscore = zscore_anomaly(df_injected)
    df_anomalous_instances_zscore = get_list_of_anomalies(df_final_zscore)

    #Determining the number of synthetic anomaly detected by Prophet
    df_final_prophet = prophet_anomaly_trained(df_injected)
    df_anomalous_instances_prophet = get_list_of_anomalies(df_final_prophet)


    total_number_of_synthetic_anomaly.append(number_of_anomalies)

    list_of_anomalies_found_by_boxplot.append(len(df_anomalous_instances_boxplot))
    list_of_anomalies_found_by_zscore.append(len(df_anomalous_instances_zscore))
    list_of_anomalies_found_by_prophet.append(len(df_anomalous_instances_prophet))

    list_of_synthetic_anomalies_found_by_boxplot.append(len(df_anomalous_instances_boxplot.loc[df_anomalous_instances_boxplot["synthetic_anomaly"] == True, "anomaly"] == "Yes")) 
    list_of_synthetic_anomalies_found_by_zscore.append(len(df_anomalous_instances_zscore.loc[df_anomalous_instances_zscore["synthetic_anomaly"] == True, "anomaly"] == "Yes"))
    list_of_synthetic_anomalies_found_by_prophet.append(len(df_anomalous_instances_prophet.loc[df_anomalous_instances_prophet["synthetic_anomaly"] == True, "anomaly"] == "Yes"))

    print("No of iteration passed: " + str(i))

data = {'total_synthetic_anomaly': total_number_of_synthetic_anomaly,
        'no_anomaly_boxplot_found': list_of_anomalies_found_by_boxplot,
        'no_synthetic_anomaly_boxplot_found': list_of_synthetic_anomalies_found_by_boxplot, 
        'no_anomaly_zscore_found': list_of_anomalies_found_by_zscore,
        'no_synthetic_anomaly_zscore_found':list_of_synthetic_anomalies_found_by_zscore, 
        'no_anomaly_prophet_found': list_of_anomalies_found_by_prophet,
        'no_synthetic_anomaly_prophet_found': list_of_synthetic_anomalies_found_by_prophet
        }

df = pd.DataFrame(data)
df.to_csv("Dataset/result/" + dataset_name + ".csv", encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
print("Program Completed Successfully")

    # print("The number of anomalies found by Boxplot: " + str(len(df_anomalous_instances_boxplot)) + ", out of which " + str(len(df_anomalous_instances_boxplot.loc[df_anomalous_instances_boxplot["synthetic_anomaly"] == True, "anomaly"] == "Yes")) + " are synthetic anomaly.")
    # print("The number of anomalies found by Z score: " + str(len(df_anomalous_instances_zscore)) + ", out of which " + str(len(df_anomalous_instances_zscore.loc[df_anomalous_instances_zscore["synthetic_anomaly"] == True, "anomaly"] == "Yes")) + " are synthetic anomaly.")
    # print("The number of anomalies found by Prophet: " + str(len(df_anomalous_instances_prophet)) + ", out of which " + str(len(df_anomalous_instances_prophet.loc[df_anomalous_instances_prophet["synthetic_anomaly"] == True, "anomaly"] == "Yes")) + " are synthetic anomaly.")


