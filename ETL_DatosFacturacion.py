import pyodbc
import psycopg2
import json
from time import time


def conectar_SQL():
    direccion_servidor = '74.208.235.157'
    # direccion_servidor = 'localhost'
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

with conexion_POST.cursor() as cursor_POST:

    #Información de facturación de alumnos v2:
    select_ = "select talumno.aluid, alunocontrol, alufac, alufacrfc, alufacrazons, alufacdir, alufacemail, alufacregimen, aacfcurp "
    select_ += " from talumno inner join taalumnoconexion on talumno.aluid=taalumnoconexion.aluid order by aluid;"

    cursor_POST.execute(select_)
    Alumnos_DF = cursor_POST.fetchall()

    for alumno in Alumnos_DF:
        print(str(alumno[1]).strip())
        #Actualizar información de CURP:
        if str(alumno[8]).strip() != 'None' and str(alumno[8]).strip() != '' and str(alumno[8]).strip() != 'NoneType':
            update_CURP = "update Alumno set AL_CURP='" + str(alumno[8]).strip() + "' where AL_Matricula='" + str(alumno[1]).strip() + "';"
            cursor_sql.execute(update_CURP)
            cursor_sql.commit()
        
        #Actualizar información de si se requiere factura:
        AL_Factura_ = str(alumno[2]).strip() if str(alumno[2]).strip() != 'None' and str(alumno[2]).strip() != '' and str(alumno[2]).strip() != 'NoneType' else "0"
        update_FAC = "update Alumno set AL_Factura=" + AL_Factura_ + " where AL_Matricula='" + str(alumno[1]).strip() + "';"
        cursor_sql.execute(update_FAC)
        cursor_sql.commit()

        #Transformar datos de facturacion        
        DF_RFC_= str(alumno[3]).strip() if str(alumno[3]).strip() != 'None' and str(alumno[3]).strip() != '' and str(alumno[3]).strip() != 'NoneType' else ""
        DF_Nombre_= str(alumno[4]).strip() if str(alumno[4]).strip() != 'None' and str(alumno[4]).strip() != '' and str(alumno[4]).strip() != 'NoneType' else ""
        DF_Email_= str(alumno[6]).strip() if str(alumno[6]).strip() != 'None' and str(alumno[6]).strip() != '' and str(alumno[6]).strip() != 'NoneType' else ""

        DF_Uso_ = str(alumno[7]).strip() if str(alumno[7]).strip() != 'None' and str(alumno[7]).strip() != '' and str(alumno[7]).strip() != 'NoneType' else ""
        DF_Uso_ = "000" if DF_Uso_ == "REGIMEN SIMPLIFICADO DE CONFIANZA" else DF_Uso_[0:3]
        DF_Direccion_ = str(alumno[5]).strip() if str(alumno[5]).strip() != 'None' and str(alumno[5]).strip() != '' and str(alumno[5]).strip() != 'NoneType' else ""

    
        if DF_RFC_ != "" and DF_RFC_ != "No lo tengo a la man":
            #Agregar datos de facturación:
            select_ALID = "select AL_ID from Alumno where AL_Matricula='" + str(alumno[1]).strip() + "';"
            cursor_sql.execute(select_ALID)
            AL_ID = cursor_sql.fetchall()
            if len(AL_ID) > 0:
                AL_Id = AL_ID[0][0]

                insert_facturacion = "INSERT INTO [dbo].[DatosFacturacion] ([DF_RFC],[DF_Nombre], [DF_Uso], [DF_Email], [AL_ID],  [DF_Direccion]) VALUES('"
                insert_facturacion += DF_RFC_ +  "','" + DF_Nombre_ + "','" + DF_Uso_ + "','" + DF_Email_ + "'," + str(AL_Id).strip() + ",'" + DF_Direccion_  +  "');"

                cursor_sql.execute(insert_facturacion)
                cursor_sql.commit()

cursor_POST.close()
cursor_sql.close()
conexion_POST.close()
conexion_SQL.close()        