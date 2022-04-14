#!/usr/bin/env python
# coding: utf-8

# ## Creación Tablas en Redshift Query Editor

# In[ ]:


DROP TABLE  IF EXISTS public.Dim_Fecha;
DROP TABLE  IF EXISTS public.Dim_Pais;
DROP TABLE  IF EXISTS public.Dim_Usuario;
DROP TABLE  IF EXISTS public.Dim_Vendedor;
DROP TABLE  IF EXISTS public.Dim_Contrato;
DROP TABLE  IF EXISTS public.Dim_Dispositivo;
DROP TABLE  IF EXISTS public.Dim_Vehiculo;
DROP TABLE  IF EXISTS public.Dim_Cliente;
DROP TABLE  IF EXISTS public.Dim_Costo;
DROP TABLE  IF EXISTS public.Dim_Prenovacion;
DROP TABLE  IF EXISTS public.Fact_Ventas;


CREATE TABLE IF NOT EXISTS public.dim_Fecha(
ID_Fecha  DATE NOT NULL,
Fecha_Emision	DATE NOT NULL,
Year  INTEGER NOT NULL,
month	INTEGER NOT NULL,
quarter INTEGER NOT NULL,
day INTEGER NOT NULL	,
week INTEGER NOT NULL,
dayofweek INTEGER NOT NULL,
is_weekend INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS public.dim_Pais(
ID_Pais INTEGER PRIMARY KEY,
Pais VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Dim_Usuario(
ID_Usuario INTEGER PRIMARY KEY,
Usuario VARCHAR (30) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Dim_Vendedor(
ID_Vendedor INTEGER PRIMARY KEY,
Vendedor VARCHAR (30) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Dim_Contrato(
ID_Contrato INTEGER PRIMARY KEY,
Subcontrato VARCHAR (20) NOT NULL,
Grupo  VARCHAR (20) NOT NULL,
Familia VARCHAR (30) NOT NULL,
Modalidad VARCHAR (10) NOT NULL,
Duracion VARCHAR (20) NOT NULL,
Plan VARCHAR (80) NOT NULL
);


CREATE TABLE IF NOT EXISTS public.Dim_Dispositivo(
ID_Dispositivo INTEGER PRIMARY KEY,
Equipo VARCHAR (20) NOT NULL,
No_Serie INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Dim_Vehiculo(
ID_Vehiculo INTEGER PRIMARY KEY,
Codigo_Vehiculo VARCHAR (30) NOT NULL,
Placa  VARCHAR (20) NOT NULL,
Chasis VARCHAR (25) NOT NULL,
Fabricante VARCHAR (20) NOT NULL,
Linea VARCHAR (25) NOT NULL,
Tipo VARCHAR (35) NOT NULL,
Color VARCHAR (40) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Dim_Cliente(
ID_Cliente INTEGER PRIMARY KEY,
Nombre VARCHAR (100) NOT NULL,
Codigo_Cliente  VARCHAR (20) NOT NULL,
NIT INTEGER NOT NULL,
Telefono INTEGER NOT NULL,
Correo_Electronico VARCHAR (50) NOT NULL,
Direccion VARCHAR (200) NOT NULL
);

CREATE TABLE IF NOT EXISTS public. Dim_Costo(
ID_Costo INTEGER PRIMARY KEY,
Marca VARCHAR (30) NOT NULL,
Modelo VARCHAR (30) NOT NULL,
Valor_FOB NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS public. Dim_Prenovacion(
ID_Precio INTEGER PRIMARY KEY,
Familia VARCHAR (30) NOT NULL,
Precio_Renovacion NUMERIC NOT NULL
);


create table if not exists public.Fact_Ventas(
   ID INTEGER PRIMARY KEY  
  ,ID_Fecha date not null
  ,ID_Pais INTEGER not null
  ,ID_Usuario INTEGER not null
  ,ID_Vendedor INTEGER not null
  ,ID_Contrato INTEGER not null
  ,ID_Dispositivo INTEGER not null
  ,ID_Vehiculo INTEGER not null
  ,ID_Cliente INTEGER not null
  ,ID_Costo INTEGER not null
  ,ID_Precio INTEGER not null
  ,Cantidad INTEGER not null
  ,Precio_Total numeric not null
  ,Importe numeric not null
  ,Descuento_Lineal numeric not null
  ,Impuestos numeric not null
  ,Valor_FOB numeric not null);

constraint("ventas_pk", "primary", "key", "(ID,", "ID_Fecha,", "ID_Pais,", "ID_Usuario,", "ID_Vendedor,", "ID_Contrato,", "ID_Dispositivo,", "ID_Vehiculo,", "ID_Cliente,", "ID_Costo,", "ID_Precio)")

  constraint("ventas_Fecha_fk", "foreign", "key", "(ID_Fecha)", "references", "dim_Fecha", "(ID_Fecha)")
  constraint("ventas_Pais_fk", "foreign", "key", "(ID_Pais)", "references", "Dim_Pais(ID_Pais)")
  constraint("ventas_Usuario_fk", "foreign", "key", "(ID_Usuario)", "references", "Dim_Usuario", "(ID_Usuario)")
  constraint("ventas_Vendedor_fk", "foreign", "key", "(ID_Vendedor)", "references", "Dim_Vendedor", "(ID_Vendedor)")
  constraint("ventas_Contrato_fk", "foreign", "key", "(ID_Contrato)", "references", "Dim_Contrato", "(ID_Contrato)")
  constraint("ventas_Dispositivo_fk", "foreign", "key", "(ID_Dispositivo)", "references", "Dim_Dispositivo", "(ID_Dispositivo)")
  constraint("ventas_Vehiculo_fk", "foreign", "key", "(ID_Vehiculo)", "references", "Dim_Vehiculo", "(ID_Vehiculo)")
  constraint("ventas_Cliente_fk", "foreign", "key", "(ID_Cliente)", "references", "Dim_Cliente", "(ID_Cliente)")
  constraint("ventas_Costo_fk", "foreign", "key", "(ID_Costo)", "references", "Dim_Costo", "(ID_Costo)")
  constraint("ventas_Precio_fk", "foreign", "key", "(ID_Precio)", "references", "Dim_Prenovacion", "(ID_Precio)")
)

