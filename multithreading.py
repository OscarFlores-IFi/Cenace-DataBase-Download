# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 10:46:32 2020

@author: OscarFlores-IFi
"""

#%%=========================================================================================================
#                                    Librerías necesarias para correr el código
#===========================================================================================================
import time
import warnings
import os
import os.path
from concurrent.futures import ThreadPoolExecutor, as_completed

# import socket
# from multiprocessing.pool import Pool
# from multiprocessing import freeze_support
# from os import cpu_count

from datetime import date
from datetime import timedelta
import urllib, json

import pandas as pd
import numpy as np


#%%=========================================================================================================
#                                        Definición de Funciones Utilizadas
#===========================================================================================================
# Función generadora de enlaces URL's para el Servicio Web de CENACE
def ruta_descarga(zona_sel, Inicial, Mercado):
    """Esta función se encarga de generar la ruta de descarga a partir del formato del Servicio Web de CENACE. Para
    hacer que dicha función trabaje, es necesario introducir una zona o selección de zonas (formato de arreglo) y la
    fecha inicial a partir de la cual se quieren comenzar a recoger los datos."""
   
    def Mes(fecha):
        """Esta función se encarga de agregar un 0 a los primeros 9 meses del año introduciendo como variable un elemento
        que se encuentre en formato de fecha (perteneciente a la librería "datetime")."""
        if fecha.month < 10:
            return "0" + str(fecha.month)
        else:
            return str(fecha.month)
    
    def Dia(fecha):
        """Esta función se encarga de agregar un 0 a los primeros 9 días del mes introduciendo como variable un elemento que
        se encuentre en formato de fecha (perteneciente a la librería "datetime")."""
        if fecha.day < 10:
            return "0" + str(fecha.day)
        else:
            return str(fecha.day)

    
    # Ruta base para la descarga
    ruta = "https://ws01.cenace.gob.mx:8082/SWPEND/SIM/"
    # Sistema Interconectado: Sistema Interconectado Nacional (SIN), Sistema Interconectado Baja Californa (BCA) y
    # Sistema Interconectado Baja California Sur (BCS)
    sis_int = "SIN"
    # Proceso que se va a utilizar: MDA o MTR
    proc = Mercado # MDA / MTR 
    # Se juntan las variables anteriores en una sola ruta
    ruta = ruta + sis_int + "/" + proc + "/"
    # Se agrega la zona (o las zonas) a la ruta del URL. En caso de tener múltiples zonas, se agregan comas para separar
    # los lugares que se desean analizar
    for zona in (zona_sel):
        ruta = ruta + zona + ","
    # Con la variable de fecha que se introduce (desde qué fecha se desea descargar), se agregan los datos al URL
    ruta = ruta[:-1] + "/" + str(Inicial.year) + "/" + Mes(Inicial) + "/" + Dia(Inicial) + "/"
    # Se define la fecha final con un desplazamiento de 6 días (tiempo límite de consulta con el Servicio Web = 7 días)
    Final = Inicial + timedelta(days = 6)
    # Se añaden los nuevos datos al URL
    ruta = ruta + str(Final.year) + "/" + Mes(Final) + "/" + Dia(Final) + "/"
    # Formato de salida: XML o JSON (en minúsculas)
    formato = "json"
    # Se crea el URL final de acceso al Servicio Web
    ruta = ruta + formato
    return ruta

# Función que permite extraer la información de la llamada al Servicio Web en formato JSON
def getDF(ruta):
    """Esta función se encarga de acceder a la información de Internet y extraer los datos."""
    try:
        # Se genera una variable que almacena los datos de la llamada al URL
        data = json.loads(urllib.request.urlopen(ruta).read())
        # El JSON se utiliza como diccionario y se obtiene el "key" que contiene la información necesaria.
        resultados = {key: value for key, value in data.items() if key == "Resultados"}.get("Resultados")
        # Se hace de manera iterativa en caso de que sean más de una zona de carga y se agrega a un dataframe
        df = pd.concat([pd.DataFrame(pd.DataFrame(i)["Valores"].tolist()).join(pd.DataFrame(i)["zona_carga"]) for i in resultados])
        print('.')
        return [df]
    except Exception:
        print('/')
        return [[None]]
    
    

# Esta función se encarga de renombrar las columnas originales de la llamada en JSON en las columnas finales de la base
# de datos
def Renombrar(archivo_csv):
    """Esta función se encarga de renombrar las columnas originales de la llamada en JSON."""
    archivo_csv.columns = [column.replace("pz_ene","Componente_Energia") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("pz_cng","Componente_Congestion") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("pz_per","Componente_Perdidas") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("zona_carga","Zona_de_Carga") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("pz","Precio_Zonal") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("fecha","Fecha") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("hora","Hora") for column in archivo_csv.columns]

# Esta función se encarga de añadir el día de la semana correspondiente a la fecha en la base de datos
def Dia_Semana(archivo_csv):
    """Esta función se encarga de añadir el día de la semana correspondiente (Lunes, Martes, Miércoles, etc.)."""
    days = {0:"Lunes", 1:"Martes", 2:"Miercoles", 3: "Jueves", 4:"Viernes", 5:"Sabado", 6:"Domingo"}
    archivo_csv["Fecha"] = pd.to_datetime(archivo_csv["Fecha"])
    archivo_csv["Dia_de_la_semana"] = archivo_csv["Fecha"].dt.dayofweek
    archivo_csv["Dia_de_la_semana"] = archivo_csv["Dia_de_la_semana"].apply(lambda x: days[x])
    
# Esta función se encarga de informar si el día es festivo y a qué festividad corresponde
def Festivos(archivo_csv):
    """Esta función se encarga de agregar el día festivo, tanto en fecha como en qué día festivo."""
    ruta = r"Festivos.csv"
    Fest = pd.read_csv(ruta)
    Fest["Fecha"]= pd.to_datetime(Fest["Fecha"])
    Fest["Fecha"] = Fest["Fecha"].dt.strftime("%Y-%m-%d")
    Fest["Fecha"]= pd.to_datetime(Fest["Fecha"])
    archivo_csv = pd.merge(archivo_csv, Fest, on = ["Fecha", "Zona_de_Carga"], how = "left")
    return archivo_csv


def Create_ini(Mercado, date_ = None):
    """
    Parameters
    ----------
    date_ : TYPE, optional
        DESCRIPTION. The default is None. If provided with a tuple (YYYY,MM,DD) the initial date
        will be the provided one, instead of the defaults. 

    Returns
    -------
    Fechas_ini : datetime.
        DESCRIPTION. It returns a list of datetime objects, necessary for the URL requests. 
        

    """
    def ini(Mercado, date_):
        if date_:
            return date(date_[0],date_[1],date_[2])
        
        if os.path.exists("BaseDatos.csv"):
            # Si existe una base de datos se revisa la última fecha añadida y con ello se genera una nueva fecha para el ULR
            BaseDatos = pd.read_csv("BaseDatos.csv")
            Ini = (BaseDatos["Fecha"]).max()
            # El formato de fecha "AAAA-MM-DD" de la base de datos se transforma a formato de la librería "datetime" 
            return date(int(Ini[0:4]), int(Ini[5:7]), int(Ini[8:10])) + timedelta(days = 1)
        else:
            # Se crea la fecha del primer registro de CENACE con formato (año, mes, día) en caso de que no existan archivos
            # previos. Esto permite generar la base de datos desde la primera entrada

            if Mercado == 'MDA':
                return date(2016,1,29) # 29 de enero de 2016. MDA --> Ini = date(2016,1,29)
            return date(2017,1,27) # 27 de enero de 2017. MTR --> Ini = date(2017,1,27)

    Inicial = ini(Mercado, date_)
    Fin = date.today() + timedelta(days = 1)
    
    periodos = int(np.ceil((Fin - Inicial).days/7))
    Fechas_ini = [(Inicial + timedelta(days=i*7)) for i in range(periodos)]
    return Fechas_ini


#%% Main
def main():
    #%%=========================================================================================================
    #                                        Definición de Variables Utilizadas
    #===========================================================================================================
    # Se definen las zonas que se analizan/añaden a la base de datos. Estas zonas son de la región Occidental
    Zonas = ["AGUASCALIENTES", "APATZINGAN", "CELAYA", "CIENEGA", "COLIMA", "FRESNILLO", "GUADALAJARA", "IRAPUATO",
               "IXMIQUILPAN", "JIQUILPAN", "LEON", "LOS-ALTOS", "MANZANILLO", "MATEHUALA", "MINAS", "MORELIA", "QUERETARO",
               "SALVATIERRA", "SAN-JUAN-DEL-RIO", "SAN-LUIS-POTOSI", "TEPIC-VALLARTA", "URUAPAN", "ZACAPU", "ZACATECAS",
               "ZAMORA", "ZAPOTLAN"]
    
    # Se definen los nombres de las columnas recogidas de la llamada URL en formato JSON del Servicio Web de CENACE
    Columnas = ["fecha", "hora", "pz", "pz_cng", "pz_ene", "pz_per", "zona_carga", "Mercado", "Dia_de_la_semana", "Festivo"]
    
    
    #%%=========================================================================================================
    #                                        Descarga y limpieza de base de datos
    #===========================================================================================================
    warnings.simplefilter("ignore")
    
    t1 = time.time()
    # Se crea el data frame de pandas con las Columnas definidas en la sección de variables
    BaseDatos = pd.DataFrame(columns = Columnas[:-2])
    
    # Se descargan los precios desde Ini hasta hoy
    Ini = None # (2020,1,1) / None # Tupla o None, de otra forma dará error. Si se especifíca una fecha, se descargarán los datos desde ella hasta el presente
    IniMDA = Create_ini('MDA', Ini) 
    IniMTR = Create_ini('MTR', Ini) # Ini = Create_ini((2020,1,1)) # Para descargar datos desde la fecha señalada hasta 'hoy'
    
    zona_sel = [ Zonas[10 * i : 10 * i + 10] for i in range(1 + len(Zonas)//10)]
     
    descMDA = [ruta_descarga(z,i,'MDA') for i in IniMDA for z in zona_sel]
    descMTR = [ruta_descarga(z,i,'MTR') for i in IniMTR for z in zona_sel]
    desc = descMDA + descMTR
    #%%                Descarga de base de datos
    t1 = time.time()
    
    processes = []
    # with ThreadPoolExecutor(max_workers=cpu_count()*2) as executor:
    with ThreadPoolExecutor(max_workers=32) as executor:
        for d in desc:
            processes.append(executor.submit(getDF, d))
    
    t2 = time.time()
    print('requests have been submited \n Time elapsed ' + str(t2-t1) + ' seconds')
    
    for task in as_completed(processes):
        print(task.result())
    
    t3 = time.time()
    print('requests have been received \n Time elapsed ' + str(t3-t2) + ' seconds')
    
    
    tmp = 0
    for proc in processes:
        try:
            if tmp <= len(descMDA):
                proc.result()[0]["Mercado"] = "MDA"
            else:
                proc.result()[0]["Mercado"] = "MTR"
        
            BaseDatos = BaseDatos.append(proc.result()[0][Columnas[:-2]], ignore_index=True)
            tmp += 1
        except Exception:
            pass
        
    
    
    #%%
    Renombrar(BaseDatos)
    Dia_Semana(BaseDatos)
    BaseDatos = Festivos(BaseDatos)
    print("New data points: " + str(len(BaseDatos)))
    
    if len(BaseDatos) > 0:
        try:
            BaseAntigua = pd.read_csv('BaseDatos.csv', index_col=0)
            BaseDatos = BaseAntigua.append(BaseDatos)
        except:
            pass
        print("Total data points: " + str(len(BaseDatos)))
        BaseDatos.to_csv('BaseDatos.csv')
        
        t4 = time.time()
        print('data has been processed. \n Time elapsed ' + str(t4-t3) + ' seconds')
    else:
        t4 = time.time()        
        print('No new data to be processed.')
        
   
    print(f'Task finished. \n Total time elapsed: {t4 - t1} seconds')


    # https://stackoverflow.com/questions/28631288/concurrent-futures-works-well-in-command-line-not-when-compiled-with-pyinstal

if __name__ == "__main__":
    # freeze_support()
    main()
    time.sleep(20)





