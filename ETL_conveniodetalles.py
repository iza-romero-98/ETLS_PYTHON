import pyodbc
import psycopg2
import json
from time import time


def conectar_SQL():
    # direccion_servidor = '217.160.15.235'
    # direccion_servidor = 'localhost'
    direccion_servidor = '74.208.235.157'
    nombre_bd = 'InterERPv3P'
    nombre_usuario = 'sa'
    password = 'Root.inter2020!'

    try:
        conexion_SQL = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                                direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
        print("OK! conexión exitosa")
    except Exception as e:
        # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)
    
    return conexion_SQL


def conectar_POST():
    # Recomendado: https://parzibyte.me/blog/2018/12/20/args-kwargs-python/
    try:
        credenciales = {
            "dbname": "InterERP",
            "user": "postgres",
            "password": "Inter.r00t19!",
            "host": "interpue.com.mx",
            # "host": "localhost",
            "port": 5435
        }
        conexion_POST = psycopg2.connect(**credenciales)    
        print("OK! conexión exitosa - POSTGRESQL")

    except psycopg2.Error as e:
        print("Ocurrió un error al conectar a PostgreSQL: ", e)

    return conexion_POST




###################################################################3
####Main__ apartir de aquí corren los procesos 

conexion_SQL = conectar_SQL()
conexion_POST = conectar_POST()

cursor_sql = conexion_SQL.cursor() #Cursos para ejecutar consultas


# Cursor PostgreSQL para tener la información que se insertará
with conexion_POST.cursor() as cursor_post:
    start_time = time() #Medir tiempo
    select_ = "select * from ccconveniosdetalle;"

    cursor_post.execute(select_)
    detalles_ = cursor_post.fetchall()

    for detalle in detalles_:

        insert_ = "INSERT INTO [dbo].[DetalleConvenio] ([DC_Convenio],[DC_CuentaDetalle],[DC_Usuid]) VALUES ("
        insert_ += str(detalle[0]).strip() + "," + str(detalle[1]).strip() + ",9);"

        print(insert_)
        cursor_sql.execute(insert_)
        cursor_sql.commit()

elapsed_time = time() - start_time
print("Elapsed time: %.10f seconds." % elapsed_time) 
cursor_sql.close()
cursor_post.close()