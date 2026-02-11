# -*- coding: utf-8 -*-
"""
Created on Tue Apr 22 09:26:15 2025

@author: DigIT-DuS6
"""

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from skforecast.sarimax import Sarimax
from skforecast.recursive import ForecasterSarimax
import copy


data = pd.read_csv("C:\\Users\\Digit-DUS6\\Downloads\\Geschwindigkeitsdisplays_2024.csv", delimiter=";")
data['datum'] = pd.to_datetime(data['datum'])
data['time'] = data['datum'].dt.time

orte = ['Allmannsdorfer Straße', 'Alter Bannweg', 'Am Pfeiferhölzle',
 'Breslauer Straße', 'Bruder-Klaus-Staße', 'Eichhornstraße',
 'Grundschule Wollmatingen', 'Hoheneggstraße', 'Jacob-Burckhardt-Straße',
 'Kindlebildstraße', 'Längerbohlstraße', 'Mainaustraße Hoehe Schule',
 'Mainaustraße', 'Radolfzeller Straße', 'Reichenaustraße', 'Schiffstraße',
 'Schwaketenstraße', 'Taborweg', 'Untere Laube', 'Wollmatinger Straße']

agg_data = []

data.set_index('datum', inplace = True)
data.sort_index(inplace = True)
#test = data['hausnummer'].unique()

for ort in orte:
    
    #Filter by street
    df_spec = data[data["strasse"]==ort]
    
    #Aggregrate Data
    grouping = df_spec.groupby(['richtung']) #, 'info', 'strasse', 'hausnummer'
    aggregate = grouping.agg({'anzahl_fahrzeuge': ['sum', 'mean'],
    'anzahl_messungen': ['mean'],
    'durchschnittsgeschwindigkeit': ['mean'],
    'hoechstgeschwindigkeit': ['mean']}).reset_index()
    
    agg_data.append(copy.copy(aggregate))
    
    intervention = round(aggregate.iloc[0][4], 2)
    nointervention =  round(aggregate.iloc[1][4], 2)
    ##1 Linear Regression
    
    #Prepare Data
    df_linearreg = df_spec.sort_values("richtung")
    
    #Richtung als kategorische Variable mit True/False codieren
    richtung = pd.get_dummies(df_linearreg["richtung"])
    richtung.drop(2, axis='columns', inplace=True)
    
    #Abhaengige Variable rausfiltern
    df_linearreg_dep = df_linearreg["durchschnittsgeschwindigkeit"]
    df_linearreg_dep.sort_values()

    #Splitting into test and train Data
    X_Train, X_Test, Y_Train, Y_Test = train_test_split(richtung, df_linearreg_dep, test_size = 2/3, random_state = 0)
    
    #Linear regression
    reg = LinearRegression()
    reg.fit(richtung, df_linearreg_dep)
    
    #Get coefficients
    slope = reg.coef_
    intercept = reg.intercept_
    
    #Linear regression prediction
    y_pred = reg.predict(richtung)
    
    #Visualize the results Linear Regression
    plt.scatter(range(len(df_linearreg_dep)), df_linearreg_dep, color='blue', label='Average Speed')  # Actual data points
    plt.plot(range(len(df_linearreg_dep)), y_pred, color='red', label='Regression Line')  # Predicted regression line
    plt.xlabel('Messungen')
    plt.ylabel('Average Speed')
    plt.legend()
    plt.title(f'{ort}\nSlope: {slope} | Intercept: {intercept}\nIntervention: {intervention} | No Intervention: {nointervention}')
    plt.draw()
    
    # Display the model's coefficients
    print(f'Slope (Coefficients): {slope}')
    print(f'Intercept: {intercept}')
    
    #ARIMA Regression
    df_arima = df_spec.drop(["anzahl_messungen", "hoechstgeschwindigkeit", "hausnummer"], axis=1)
    df_intervention = df_arima[df_arima["richtung"] == 1]
    df_nointervention = df_arima[df_arima["richtung"] == 2]
    
    #remove duplicates
    df_intervention = df_intervention.groupby(df_intervention.index).last()
    df_nointervention = df_nointervention.groupby(df_nointervention.index).last()
    
    df_intervention.drop("richtung", axis=1, inplace=True)
    df_nointervention.drop("richtung", axis=1, inplace=True)
    
    df_intervention = df_intervention.resample('30min').asfreq().ffill()
    df_nointervention = df_nointervention.resample('30min').asfreq().ffill()

    train_per = int(len(df_intervention)*0.9)
    train_data_int = df_intervention.iloc[:(200-train_per)]
    train_lastwindow_int = df_intervention.iloc[(200-train_per):train_per]
    test_data_int = df_intervention[train_per:]
    train_data_noint = df_nointervention.iloc[:(200-train_per)]
    train_lastwindow_noint = df_nointervention.iloc[(200-train_per):train_per]
    test_data_noint = df_nointervention[train_per:]
    
    #train_data_int = train_data_int.resample('30min').asfreq().ffill()
    #test_data_int = test_data_int.resample('30min').asfreq().ffill()
    #train_data_noint = train_data_noint.resample('30min').asfreq().ffill()
    #test_data_noint = test_data_noint.resample('30min').asfreq().ffill()
    
    arima_int = Sarimax(order=(1, 1, 1))
    arima_int.fit(y=train_data_int['durchschnittsgeschwindigkeit'])
    arima_noint = Sarimax(order=(1, 1, 1))
    arima_noint.fit(y=train_data_noint['durchschnittsgeschwindigkeit'])
    
    forecaster_int = ForecasterSarimax(arima_int)
    forecaster_noint = ForecasterSarimax(arima_noint)

    forecaster_int.fit(y=train_data_int['durchschnittsgeschwindigkeit'], exog=train_data_int['anzahl_fahrzeuge'], suppress_warnings=True)
    forecaster_noint.fit(y=train_data_noint['durchschnittsgeschwindigkeit'],exog=train_data_noint['anzahl_fahrzeuge'], suppress_warnings=True)
    
    steps_int = len(test_data_int)
    steps_noint = len(test_data_noint)
    
    predictions_int = forecaster_int.predict(steps=steps_int, exog=test_data_int['anzahl_fahrzeuge'], last_window=train_lastwindow_int['durchschnittsgeschwindigkeit'], last_window_exog=train_lastwindow_int['anzahl_fahrzeuge'])
    predictions_noint = forecaster_noint.predict(steps=steps_noint, exog=test_data_noint['anzahl_fahrzeuge'], last_window=train_lastwindow_noint['durchschnittsgeschwindigkeit'], last_window_exog=train_lastwindow_noint['anzahl_fahrzeuge'])
    
    meanint = round(predictions_int.mean(), 2)
    meannoint = round(predictions_noint.mean(), 2)
    
    fig, ax = plt.subplots(figsize=(7, 3))
    #train_data['durchschnittsgeschwindigkeit'][:200].plot(ax=ax, label='train')
    #test_data_int['durchschnittsgeschwindigkeit'].plot(ax=ax, label='data_intervention')
    #test_data_noint['durchschnittsgeschwindigkeit'].plot(ax=ax, label='data_nointervention')
    test_data_int['anzahl_fahrzeuge'].plot(ax=ax, label='Anzahl Fahrzeuge')
    predictions_int.plot(ax=ax, label='predictions_intervention')
    predictions_noint.plot(ax=ax, label='predictions_no_intervention')
    plt.title(f'{ort}\nprediction_intervention: {meanint} | prediction_no_intervention: {meannoint}')
    ax.legend()
    plt.draw()
    plt.show()

#df_allmannsdorferstrasse["richtung"] = richtung
#x = df_allmannsdorferstrasse[["richtung"]]
# plot
#fig, ax = plt.subplots()
#ax.bar(x, y, width=1, edgecolor="white", linewidth=0.7)
#ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
#ylim=(0, 8), yticks=np.arange(1, 8))
#plt.show()

    
    
    
    
