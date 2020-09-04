# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 10:46:32 2020

@author: OF
"""

#%%=========================================================================================================
#                                    Librerías necesarias para correr el código
#===========================================================================================================
import time
# from multiprocessing.pool import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed

from os import cpu_count
import os.path
from datetime import date
from datetime import timedelta
import urllib, json

import pandas as pd
import numpy as np



#%%=========================================================================================================
#                                        Definición de Variables Utilizadas
#===========================================================================================================
# Se definen las zonas que se analizan/añaden a la base de datos. Estas zonas son de la región Occidental
Zonas = ["AGUASCALIENTES", "APATZINGAN", "CELAYA", "CIENEGA", "COLIMA", "FRESNILLO", "GUADALAJARA", "IRAPUATO",
           "IXMIQUILPAN", "JIQUILPAN", "LEON", "LOS-ALTOS", "MANZANILLO", "MATEHUALA", "MINAS", "MORELIA", "QUERETARO",
           "SALVATIERRA", "SAN-JUAN-DEL-RIO", "SAN-LUIS-POTOSI", "TEPIC-VALLARTA", "URUAPAN", "ZACAPU", "ZACATECAS",
           "ZAMORA", "ZAPOTLAN"]

# Se definen los nombres de las columnas recogidas de la llamada URL en formato JSON del Servicio Web de CENACE
Columnas = ["fecha", "hora", "pz", "pz_cng", "pz_ene", "pz_per", "zona_carga", "Dia_de_la_semana", "Festivo"]


#%%=========================================================================================================
#                                        Definición de Funciones Utilizadas
#===========================================================================================================
# Función generadora de enlaces URL's para el Servicio Web de CENACE
def ruta_descarga(zona_sel, Inicial):
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
    proc = "MTR"
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
    print(ruta)
    return ruta

# Función que permite extraer la información de la llamada al Servicio Web en formato JSON
def getDF(ruta):
    """Esta función se encarga de acceder a la información de Internet y extraer los datos."""
    try:
        print('.')
        # Se genera una variable que almacena los datos de la llamada al URL
        data = json.loads(urllib.request.urlopen(ruta).read())
        # El JSON se utiliza como diccionario y se obtiene el "key" que contiene la información necesaria.
        resultados = {key: value for key, value in data.items() if key == "Resultados"}.get("Resultados")
        # Se hace de manera iterativa en caso de que sean más de una zona de carga y se agrega a un dataframe
        df = pd.concat([pd.DataFrame(pd.DataFrame(i)["Valores"].tolist()).join(pd.DataFrame(i)["zona_carga"]) for i in resultados])
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

# Esta función se encarga de remplazar los nombres de las columnas de la base de datos final a las columnas iniciales
# de la llamada en JSON
def RenombrarInv(archivo_csv):
    """Esta función es inversa a la función 'Renombrar' y se encarga de regresar los nombres de las columnas originales.
    Este cambio es necesario cuando se requiere añadir información nueva a la base de datos existente."""
    archivo_csv.columns = [column.replace("Precio_Zonal","pz") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Componente_Congestion","pz_cng") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Componente_Energia","pz_ene") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Componente_Perdidas","pz_per") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Zona_de_Carga","zona_carga") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Fecha","fecha") for column in archivo_csv.columns]
    archivo_csv.columns = [column.replace("Hora",'hora') for column in archivo_csv.columns]

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


def Create_ini(date_ = None):
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
    def ini(date_):
        if date_:
            return date(date_[0],date_[1],date_[2])
        
        if os.path.exists("BaseDatos.csv"):
            # Si existe una base de datos se revisa la última fecha añadida y con ello se genera una nueva fecha para el ULR
            BaseDatos = pd.read_csv("BaseDatos.csv")
            RenombrarInv(BaseDatos)
            BaseDatos = BaseDatos[Columnas]
            Ini = (BaseDatos["fecha"]).max()
            # El formato de fecha "AAAA-MM-DD" de la base de datos se transforma a formato de la librería "datetime" 
            Ini = date(int(Ini[0:4]), int(Ini[5:7]), int(Ini[8:10])) + timedelta(days = 1)
        else:
            # Se crea la fecha del primer registro de CENACE con formato (año, mes, día) en caso de que no existan archivos
            # previos. Esto permite generar la base de datos desde la primera entrada
            
            # 27 de enero de 2017. MTR --> Ini = date(2017,1,27)
            # 29 de enero de 2016. MDA --> Ini = date(2016,1,29)
            Ini = date(2017,1,27)
        # Se seleccionan 10 zonas para agregarlas al URL y posteriormente a la Base de Datos
        return Ini

    Inicial = ini(date_)
    Fin = date.today() + timedelta(days = 1)
    
    periodos = int(np.ceil((Fin - Inicial).days/7))
    Fechas_ini = [(Inicial + timedelta(days=i*7)) for i in range(periodos)]
    return Fechas_ini


#%%=========================================================================================================
#                                        Descarga y limpieza de base de datos
#===========================================================================================================
t1 = time.time()
# Se crea el data frame de pandas con las Columnas definidas en la sección de variables
BaseDatos = pd.DataFrame(columns = Columnas[:-2])

# Se descargan los precios desde Ini hasta hoy
# Ini = Create_ini((2020,1,1)) #puede no tener argumentos # Create_ini()
Ini = Create_ini() # Ini = Create_ini((2020,1,1)) # Para descargar datos desde la fecha señalada hasta 'hoy'

zona_sel = [ Zonas[10 * i : 10 * i + 10] for i in range(1 + len(Zonas)//10)]

desc = [ruta_descarga(z,i) for i in Ini for z in zona_sel]
        
#%%                Descarga de base de datos
t1 = time.time()

processes = []
with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
    for d in desc:
        processes.append(executor.submit(getDF, d))

for task in as_completed(processes):
    print(task.result())

for proc in processes:
    try:
        proc.result()[0]
        BaseDatos = BaseDatos.append(proc.result()[0][Columnas[:-2]], ignore_index=True)
    except Exception:
        pass
print(f'Time elapsed: {time.time() - t1} seconds')

#%%
Renombrar(BaseDatos)
Dia_Semana(BaseDatos)
BaseDatos = Festivos(BaseDatos)
print(len(BaseDatos))

try:
    BaseAntigua = pd.read_csv('BaseDatos.csv', index_col=0)
    BaseDatos = BaseAntigua.append(BaseDatos)
except:
    pass
print(len(BaseDatos))
BaseAntigua.to_csv('BaseDatos.csv')
