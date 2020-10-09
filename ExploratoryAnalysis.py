# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 18:06:41 2020

@author: OscarFlores-IFi
"""

import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt

def load_data(filename):
    Data = pd.read_csv(filename)
    Data = Data.drop(Data.columns[0], axis=1)
    Data = Data.loc[Data.Hora<25]
    Data['Hora'] = Data['Hora']-1
    Data['fecha_hora'] = pd.to_datetime(pd.to_datetime(Data.Fecha,dayfirst=True) + pd.to_timedelta(Data.Hora, unit='h'),infer_datetime_format=True)
    
    Data = Data.drop(['Hora', 'Fecha'],axis=1)
    return Data

def join_mda_mtr(data):

    mtr = data[data.Mercado=='MTR'].copy().drop('Mercado', axis=1)
    mtr.columns = ['Precio_Zonal_MTR', 'Componente_Congestion_MTR',
            'Componente_Energia_MTR', 'Componente_Perdidas_MTR', 'Zona_de_Carga',
            'Dia_de_la_semana', 'Festivo', 'fecha_hora']
    mda = data[data.Mercado=='MDA'].copy().drop('Mercado', axis=1)
    mda.columns = ['Precio_Zonal_MDA', 'Componente_Congestion_MDA',
            'Componente_Energia_MDA', 'Componente_Perdidas_MDA', 'Zona_de_Carga',
            'Dia_de_la_semana', 'Festivo', 'fecha_hora']
    
    mda.set_index = 'fecha_hora'
    mtr.set_index = 'fecha_hora'
    
    common_cols = ['Zona_de_Carga', 'Dia_de_la_semana', 'Festivo', 'fecha_hora']
    Joined = mda.merge(mtr, on = common_cols)
    return Joined


def percentual_error(y_true, y_pred): 
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return ((y_true - y_pred) / y_true)*100

##############################################################################
filename='BaseDatos.csv'
Data = load_data(filename)

zonas = Data.Zona_de_Carga.unique()
data = Data.loc[(Data.Zona_de_Carga==zonas[20])]

list_joined = [join_mda_mtr(Data.loc[(Data.Zona_de_Carga==i)]) for i in zonas]
DataZonas = pd.concat(list_joined, keys=zonas)

DataZonas.to_csv('BaseDatosJ.csv')
    
    
# DataZonas['MAPE_Precio_Zonal'] = percentual_error(DataZonas.Precio_Zonal_MTR.values, DataZonas.Precio_Zonal_MDA.values)
# correlaciones = DataZonas.corr()

# plt.hist(DataZonas.Precio_Zonal_MDA, bins = 1000)
# plt.hist(DataZonas.Precio_Zonal_MTR, bins = 1000)
# plt.show()

# plt.hist(DataZonas.MAPE_Precio_Zonal.values, bins = 1000)
# plt.show()

