#!/usr/bin/env python
# coding: utf-8

# ### Inicialización de Librerías

# In[59]:


import pandas as pd
import numpy as np
import os
import io
from pathlib import Path
import boto3
import configparser


# ### Conexión a Base RDS

# In[60]:


DB_ENDPOINT = 'ventas.cnyzxxljvw5b.us-east-1.rds.amazonaws.com' 
DB = 'ventas' 
DB_USER = 'admin'
DB_PASSWORD = 'baseaws1'
DB_PORT = '3306'


# In[61]:


mysql_conn = 'mysql+pymysql://{}:{}@{}/{}'.format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB)
print(mysql_conn)


# In[62]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[63]:


get_ipython().run_line_magic('sql', '$mysql_conn')


# In[64]:


sql_query = 'SELECT * FROM ventasgp;'
df = pd.read_sql(sql_query, mysql_conn)


# In[65]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM ventasgp;')


# ### Conexión a S3

# In[66]:


enlace ="https://444049155220.signin.aws.amazon.com/console"
secreta = "z620AEaWBmaumT2GqYmKVMMbnR++aWBkcJ2E7m7t"
acceso = "AKIAWOY3EMSKHD3JYDO2"


# In[67]:


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = acceso,
    aws_secret_access_key = secreta
)


# In[68]:


for bucket in s3.buckets.all():
    print(bucket.name)


# In[69]:


S3_BUCKET_NAME = 'datacyr'


# In[70]:


fileList = os.listdir('DataProyectoPy/S3/') 
#fileList


# In[71]:


for file in fileList:
    if(file != '.ipynb_checkpoints'):
        path = './DataProyectoPy/S3/' + file
        print(path)
        s3.Bucket(S3_BUCKET_NAME).upload_file(Filename=path, Key='datacomp/' + file)


# ### Consumo de Datos S3

# In[72]:


remoteFileList = []
for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
     remoteFileList.append(objt.key)
print(remoteFileList)


# In[73]:


dfcos = pd.DataFrame()
dfren = pd.DataFrame()

for remoteFile in remoteFileList:
    if('costos.csv' in remoteFile):
        print(remoteFile)
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        body = file["Body"].read()
        dfcos = pd.read_csv(io.BytesIO(body), sep=";", encoding = 'unicode_escape')
    elif("precios.csv" in remoteFile):
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        body = file["Body"].read()
        dfren = pd.read_csv(io.BytesIO(body), sep=";", encoding = 'unicode_escape')#, skiprows=8)


# # Creación del Modelo

# ## Dimensiones

# In[74]:


sql_query = 'SELECT * FROM ventasgp;'
df = pd.read_sql(sql_query, mysql_conn)


# ### Dim_Vendedor

# In[75]:


df_Vendedor = pd.DataFrame({"Vendedor" : df["Agente"].unique()})
Dim_Vendedor = pd.DataFrame({"ID_Vendedor" : np.arange(1, len(df_Vendedor)+1, 1) ,"Vendedor" : df["Agente"].unique()})
Dim_Vendedor


# ### Dim_Vehiculo

# In[76]:


dfVehiculo = pd.DataFrame({"Codigo_Vehiculo":df["Vehiculo"] ,"Placa" : df["Placas"], "Chasis" : df["Chasis"], "Fabricante" : df["Fabricante"], "Línea" : df["Lineas"].str.upper(), "Tipo" : df["Trackertipo"], "Color" : df["Color"] })
Dim_Vehiculo = pd.DataFrame({"ID_Vehiculo" : np.arange(1, len(dfVehiculo)+1,1), "Codigo_Vehiculo":df["Vehiculo"], "Placa" : df["Placas"], "Chasis" : df["Chasis"], "Fabricante" : df["Fabricante"], "Linea" : df["Lineas"], "Tipo" : df["Trackertipo"], "Color" : df["Color"] })
Dim_Vehiculo = Dim_Vehiculo.drop_duplicates(["Placa"])
Dim_Vehiculo


# ### Dim_Pais

# In[77]:


df_Pais = pd.DataFrame({"Pais" : df["Pais"].unique()})
Dim_Pais = pd.DataFrame({"ID_Pais": np.arange(1, len(df_Pais)+1,1),"Pais" : df["Pais"].unique()})
Dim_Pais


# ### Dim_Contrato

# In[78]:


dfContrato = pd.DataFrame({"Subcontrato" : df["Sub_Contrato"], "Grupo" : df["Grupo"], "Familia" : df["Familia"], "Modalidad" : df["Tipo_Contrato"], "Duracion" : df["Tipo_Periodo"], "Plan" : df['Descripcion_Producto'] })
Dim_Contrato = pd.DataFrame({"ID_Contrato" : np.arange(1, len(dfContrato)+1,1), "Subcontrato" : df["Sub_Contrato"], "Grupo" : df["Grupo"], "Familia" : df["Familia"], "Modalidad" : df["Tipo_Contrato"], "Duracion" : df["Tipo_Periodo"], "Plan" : df['Descripcion_Producto'] })
Dim_Contrato = Dim_Contrato.drop_duplicates()
Dim_Contrato


# ### Dim_Dispositivo

# In[79]:


dfDispositivo = pd.DataFrame({"Equipo" : df["Equipo"], "No_Serie" : df["Serial"]})
Dim_Dispositivo = pd.DataFrame({"ID_Dispositivo" : np.arange(1, len(dfDispositivo)+1,1), "Equipo" : df["Equipo"], "No_Serie" : df["Serial"]})
Dim_Dispositivo = Dim_Dispositivo.drop_duplicates(["No_Serie"])
Dim_Dispositivo


# ### Dim_Cliente

# In[80]:


dfCliente = pd.DataFrame({"Nombre" : df["Nombre"], "Codigo_Cliente" : df["Cliente"], "NIT" : df["NIT"], "Telefono" : df["Telefonos"], "Correo_Electronico": df["Email1"], "Direccion" : df["Direccion"]})
Dim_Cliente = pd.DataFrame({"ID_Cliente" : np.arange(1, len(dfCliente)+1,1),"Nombre" : df["Nombre"], "Codigo_Cliente" : df["Cliente"], "NIT" : df["NIT"], "Telefono" : df["Telefonos"], "Correo_Electronico": df["Email1"], "Direccion" : df["Direccion"]})
Dim_Cliente = Dim_Cliente.drop_duplicates(["Codigo_Cliente"])
Dim_Cliente


# ### Dim_Usuario

# In[81]:


df_Usuario = pd.DataFrame({"Usuario" : df["Usuario"].unique()})
Dim_Usuario = pd.DataFrame({"ID_Usuario" : np.arange(1, len(df_Usuario) +1, 1) ,"Usuario" : df["Usuario"].unique()})
Dim_Usuario


# ### Dim_PreciosRenovacion

# In[82]:


preno = pd.DataFrame({"Familia": dfren["Familia"] ,"Precio_Renovacion" : dfren["Precio_Renovacion"] })
Dim_Prenovacion = pd.DataFrame({"ID_Precio" : np.arange(1, len(preno)+1,1), "Familia":dfren["Familia"] ,"Precio_Renovacion" : dfren["Precio_Renovacion"]})
Dim_Prenovacion = Dim_Prenovacion.drop_duplicates(["Familia"])
Dim_Prenovacion


# ### Dim_Costos

# In[83]:


tcosto = pd.DataFrame({"Marca" : dfcos["Marca"], "Modelo":dfcos["Modelo"] ,"Valor_FOB" : dfcos["Valor_FOB"]})
Dim_Costo = pd.DataFrame({"ID_Costo" : np.arange(1, len(tcosto)+1,1), "Marca" : dfcos["Marca"], "Modelo":dfcos["Modelo"] ,"Valor_FOB" : dfcos["Valor_FOB"]})
Dim_Costo = Dim_Costo.drop_duplicates(["Modelo"])
Dim_Costo


# ### Dim Fecha

# In[84]:


df_Fecha = pd.DataFrame({"Fecha_Emision" : df["Fecha_Emision"].unique()})
#dim_Fecha = pd.DataFrame({"ID Fecha" : np.arange( 1, (len(df_Pais) + 1), 1)  ,"País" : df["País"].unique()})
df_Fecha


# In[85]:


df_Fecha['year'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).year
df_Fecha['month'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).month
df_Fecha['quarter'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).quarter
df_Fecha['day'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).day
df_Fecha['week'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).week
df_Fecha['dayofweek'] = pd.DatetimeIndex(df_Fecha['Fecha_Emision']).dayofweek
df_Fecha.head()


# In[86]:


df_Fecha['is_weekend'] = df_Fecha['dayofweek'].apply(lambda x: 1 if x > 5 else 0)
df_Fecha.head()


# In[87]:


df_Fecha['Fecha_Emision'] = pd.to_datetime(df_Fecha.Fecha_Emision, format='%Y-%M-%d')
df_Fecha['ID_Fecha'] = df_Fecha['Fecha_Emision'].dt.strftime('%Y%M%d')
Dim_Fecha = df_Fecha
Dim_Fecha


# In[88]:


df['Fecha_Emision'] = pd.to_datetime(df.Fecha_Emision  , format='%Y-%M-%d')
df['Fecha_Emision'] = df['Fecha_Emision'].dt.strftime('%Y%M%d')
#df


# ### Creación Fact Table

# In[89]:


dfV1 = df.merge(Dim_Pais, left_on='Pais', right_on='Pais')
dfV2 = dfV1.merge(Dim_Usuario, left_on ="Usuario", right_on= "Usuario")
dfV3 = dfV2.merge(Dim_Vendedor, left_on="Agente", right_on="Vendedor")
dfV4 = dfV3.merge(Dim_Contrato, left_on="Sub_Contrato", right_on="Subcontrato")
dfV5 = dfV4.merge(Dim_Dispositivo, left_on="Serial", right_on="No_Serie")
dfV6 = dfV5.merge(Dim_Vehiculo, left_on="Vehiculo", right_on="Codigo_Vehiculo")
dfV7 = dfV6.merge(Dim_Prenovacion, left_on='Familia_x', right_on='Familia')
dfV8 = dfV7.merge(Dim_Costo, left_on='Lineas', right_on='Modelo')
dfV9 = dfV8.merge(Dim_Cliente, left_on="Cliente", right_on="Codigo_Cliente")
fact_ventas = dfV9.merge (Dim_Fecha, left_on='Fecha_Emision', right_on='ID_Fecha')
fact_ventas['ID'] = range(1, len(fact_ventas)+1)
fact_ventas.rename(columns={'Familia_x': 'Familia', 'Pais_x': 'Pais','Fecha_Emision_x': 'Fecha_Emision', 'Nombre_x': 'Nombre', 'Grupo_x': 'Grupo'}, inplace=True)
fact_ventas.head()


# ### Fact_Table

# In[90]:


Fact_Ventas = fact_ventas.loc[:,["ID","ID_Fecha","ID_Pais", "ID_Usuario","ID_Vendedor","ID_Contrato","ID_Dispositivo","ID_Vehiculo", "ID_Cliente", "ID_Costo", "ID_Precio","Cantidad", "Precio_Total", "Importe", "Descuento_Lineal", "Impuestos", "Valor_FOB"]]
Fact_Ventas


# # Conexión Redshift

# In[91]:


ENDPOINT= "redshift-cluster-1.czlp9ifsh6ms.us-east-1.redshift.amazonaws.com"
DB_NAME= "dev"
DB_USER= "awsuser"
DB_PASSWORD= "redSkey1"
DB_PORT= "5439"


# In[92]:


redshift_conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, ENDPOINT, DB_PORT, DB_NAME)
print(redshift_conn_string)


# In[93]:


get_ipython().run_line_magic('sql', '$redshift_conn_string')


# In[94]:


from sqlalchemy import create_engine
conn = create_engine(redshift_conn_string)


# # Envío de los datos

# In[95]:


Dim_Fecha.to_sql('dim_fecha', conn, index=False, if_exists ='append', method = 'multi')


# In[96]:


Dim_Pais.to_sql('dim_pais', conn, index=False, if_exists ='append', method = 'multi')


# In[97]:


Dim_Usuario.to_sql('dim_usuario', conn, index=False, if_exists ='append', method = 'multi')


# In[98]:


Dim_Vendedor.to_sql('dim_vendedor', conn, index=False, if_exists ='append', method = 'multi')


# In[99]:


Dim_Contrato.to_sql('dim_contrato', conn, index=False, if_exists ='append', method = 'multi')


# In[100]:


Dim_Dispositivo.to_sql('dim_dispositivo', conn, index=False, if_exists ='append', method = 'multi')


# In[101]:


Dim_Vehiculo.to_sql('dim_vehiculo', conn, index=False, if_exists ='append', method = 'multi')


# In[102]:


Dim_Cliente.to_sql('dim_cliente', conn, index=False, if_exists ='append', method = 'multi')


# In[103]:


Dim_Costo.to_sql('dim_costo', conn, index=False, if_exists ='append', method = 'multi')


# In[104]:


Dim_Prenovacion.to_sql('dim_prenovacion', conn, index=False, if_exists ='append', method = 'multi')


# In[105]:


Fact_Ventas.to_sql('fact_ventas', conn, index=False, if_exists ='append', method = 'multi')


# ## Análisis de la Información del Modelo

# #### Pregunta 1: ¿Qué marca de vehículo es la que más se instala en la region?

# In[106]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Fabricante), sum(Importe) Importe  from fact_ventas as f\njoin dim_Vehiculo as v on f.ID_Vehiculo = v.ID_Vehiculo\ngroup by Fabricante order by Importe desc limit 10')


# #### Pregunta 2: ¿Qué país de la región es el que vende más?

# In[107]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Pais), sum(Importe) Importe  from fact_ventas as f\njoin dim_Pais as v on f.ID_Pais = v.ID_Pais\ngroup by Pais order by Importe desc')


# #### Pregunta 3: ¿Cuál es plan más vendido?

# In[108]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Plan), sum(Importe) Importe  from fact_ventas as f\njoin dim_Contrato as c on f.ID_Contrato = c.ID_Contrato\ngroup by Plan order by Importe desc limit 10')


# #### Pregunta 4: ¿Cuál es la tecnología más vendida en la región?

# In[109]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Equipo), sum(Importe) Importe  from fact_ventas as f\njoin dim_Dispositivo as d on f.ID_Dispositivo = d.ID_Dispositivo \ngroup by Equipo order by Importe desc ')


# #### Pregunta 5: ¿Qué linea de vehículo tiene mayor potencial para ser asegurado según su valor de mercado?

# In[110]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Linea), count (Linea) Cantidad , Valor_FOB from fact_ventas as f\njoin dim_Vehiculo as v on f.ID_Vehiculo = v.ID_Vehiculo group by Linea, Valor_FOB \norder by Cantidad desc limit 6')


# #### Pregunta 6: ¿Qué tipo de vehículo representará mayor potencial de ingreso por renovación de contratos el siguiente periodo?

# In[111]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Tipo), count (Tipo) Cantidad , Precio_Renovacion from fact_ventas as f\njoin dim_Vehiculo as v on f.ID_Vehiculo = v.ID_Vehiculo\njoin dim_Prenovacion as p on f.ID_Precio = p.ID_Precio \ngroup by Tipo, Precio_Renovacion order by Cantidad desc limit 10')


# #### Pregunta 7: ¿Cuál es la el promedio de ventas por categoría de cliente?

# In[112]:


get_ipython().run_cell_magic('sql', '', 'select distinct (Grupo), avg(Importe) Importe  from fact_ventas as f\njoin dim_Contrato as c on f.ID_Contrato = c.ID_Contrato\ngroup by Grupo order by Importe desc ')


# In[ ]:




