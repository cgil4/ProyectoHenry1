#importar librerias
import os  # leer y escribir un archivo
from pathlib import Path  # manipular rutas
import pandas as pd  #manejo estructura de datos
from pyspark.sql import SparkSession
from sqlite3 import connect
from sqlalchemy import create_engine
from sqlalchemy import values


df1304 = pd.read_csv('Datasets/precios_semana_20200413.csv', sep=',', encoding='utf-16') # carga de archivo
df1304 = df1304.reindex(columns=['sucursal_id','producto_id','precio']) # se ordenan columnas
df1304.rename(columns={'sucursal_id':'Sucursal_Id', 
                            'producto_id':'Producto_Id','precio':'Precio'}, 
                            inplace=True)  # se renombran columnas
prueba = df1304.isnull()  # revision de valores nulos
prueba[(prueba['Sucursal_Id'] == True)] 

df1304.dropna (how = 'all',inplace=True) # elimina columnas completas con valores nulos
df1304.dropna (inplace=True) # en vista de que solo esta el valor del precio se eliminan toda la columna con valor nulo
df1304 = df1304.fillna('sin datos')  # si hay valores nulos los reemplaza
df1304 = df1304.drop_duplicates()  # eliminado duplicados
# se crea en BD
conn = connect("proyecto_individual.db")
df1304.to_sql("df1304",conn,if_exists='append')
# Se crea Mysql

engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1') #, echo=False)
df1304.to_sql(name='df1304',con=engine, if_exists='append')

# se concatenan las dos hojas de excel en un solo dataframe
df1904 = pd.concat(pd.read_excel('Datasets/precios_semanas_20200419_20200426.xlsx',sheet_name=None),ignore_index=True) 

df1904 = df1904.drop_duplicates() # se eliminan duplicados
df1904 = df1904.fillna('sin datos') # se reemplazan valores nulos
df1904 = df1904.reindex(columns=['sucursal_id','producto_id','precio']) # se ordenan columnas
df1904.rename(columns={'sucursal_id':'Sucursal_Id', 
                            'producto_id':'Producto_Id','precio':'Precio'}, 
                            inplace=True)  # se renombran columnas
# se crea en BD
conn = connect("proyecto_individual.db")
df1904.to_sql("df1904",conn,if_exists='append')
# se crea en Mysql
engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1') #, echo=False)
df1904.to_sql(name='df1904',con=engine, if_exists='append')
df0305 = pd.read_json('Datasets/precios_semana_20200503.json') # carga de archivo
df0305 = df0305.drop_duplicates()  # se eliminan duplicados
df0305 = df0305.fillna('sin datos') # se reemplazan valores nulos
df0305 = df0305.reindex(columns=['sucursal_id','producto_id','precio']) # se ordenan columnas
df0305.rename(columns={'sucursal_id':'Sucursal_Id', 
                            'producto_id':'Producto_Id','precio':'Precio'}, 
                            inplace=True)  # se renombran columnas
# se crea en BD
conn = connect("proyecto_individual.db")
df0305.to_sql("df0305",conn,if_exists='append')
# Se crea Mysql

engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1') #, echo=False)
df0305.to_sql(name='df0305',con=engine, if_exists='append')
dfsucursal = pd.read_csv('Datasets/sucursal.csv', sep=',', encoding='utf-8') # se extrae archivo

dfsucursal = dfsucursal.drop_duplicates() # se eliminan duplicados
dfsucursal = dfsucursal.fillna('sin datos') # se reemplazan valores nulos
dfsucursal.rename(columns={'id':'Id_Principal', 
                            'comercioId':'Comercio_Id','banderaId':'Bandera_Id','banderaDescripcion':'Bandera_Descripcion','comercioRazonSocial':'Razon_Social',
                            'provincia':'Provincia','localidad':'Localidad','direccion':'Dirrecion','lat':'Latitud',
                            'lng':'Longitud','sucursalNombre':'Nombre_Sucursal','sucursalTipo':'Tipo_Sucursal'}, 
                            inplace=True)  # se renombran columnas
# se crea en BD
conn = connect("proyecto_individual.db")
dfsucursal.to_sql("dfsucursal",conn,if_exists='append')
# Se crea Mysql

engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1')
dfsucursal.to_sql(name='dfsucursal',con=engine, if_exists='append')
dfproducto = pd.read_parquet('Datasets/producto.parquet', engine='pyarrow') # se extrae archivo
dfproducto = dfproducto.drop_duplicates() # se eliminan duplicados
dfproducto = dfproducto.fillna('sin datos') # se reemplazan valosres nulos
dfproducto.rename(columns={'id':'Codigo_EAN', 
                            'marca':'Marca_Producto','nombre':'Nombre_Producto','presentacion':'Presentacion','categoria1':'Categoria1',
                            'categoria2':'Categoria2','categoria3':'Categoria3'}, 
                            inplace=True)  # se renombran columnas
# se crea en BD
conn = connect("proyecto_individual.db")
dfproducto.to_sql("dfproducto",conn,if_exists='append')
# Se crea Mysql

engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1') 
dfproducto.to_sql(name='dfproducto',con=engine, if_exists='append')
preciosunificados = pd.concat([df1304, df1904, df0305])  # se concatenan las tablas de precios
# se crea en BD
conn = connect("proyecto_individual.db")
preciosunificados.to_sql("preciosunificados",conn,if_exists='append')
# Se crea Mysql

engine = create_engine('mysql+pymysql://root:andresito@localhost/Proyecto1') 
preciosunificados.to_sql(name='preciosunificados',con=engine, if_exists='append')
preciosunificados = preciosunificados.reindex(columns=['Precio','Producto_Id','Sucursal_Id']) # se ordenan columnas
preciosunificados
