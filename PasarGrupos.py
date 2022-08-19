from select import select
from sqlite3 import Cursor
import pyodbc
import psycopg2
from time import time
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import locale

def conectar_SQL():
    direccion_servidor = '74.208.235.157'
    nombre_bd = 'InterERPv3P'
    nombre_usuario = 'sa'
    password = 'Root.inter2020!'
    try:
        conexion_SQL = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                                direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
        print("OK! conexión exitosa")
    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)
    return conexion_SQL

def AlumnosBachillerV3(sql):
    try:
        SelectAlumnos = "select AL_ID, AL_Matricula from Alumno "
        SelectAlumnos += "inner join Carreras on Alumno.AL_Carrera = Carreras.IDCarrera "
        SelectAlumnos += "where Alumno.AL_StatusActual in (21, 35, 41, 38) and Carreras.Nivel_ID = 2;"

        sql.execute(SelectAlumnos)
        
        ListaAlumnos = sql.fetchall()

        return ListaAlumnos
    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)
    

def ALUId(AL_Matricula):
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
        cursor_POST = conexion_POST.cursor()
        
        ALUId = 0
        select_ = "select aluid from talumno where alunocontrol='" + str(AL_Matricula).strip() + "';"
        cursor_POST.execute(select_)
        consulta_ = cursor_POST.fetchall()
        if len(consulta_) >  0:
            ALUId = consulta_[0][0]
        
        conexion_POST.close()
        cursor_POST.close()
        return ALUId
    except psycopg2.Error as e:
        print("Ocurrió un error al conectar a PostgreSQL: ", e)

def GrupoBachiller(AL_Matricula):

    aluid_ = ALUId(AL_Matricula)
    if aluid_ != 0:
        try:
            credenciales = {
                "dbname": "bachiller",
                "user": "postgres",
                "password": "Inter.r00t19!",
                "host": "interpue.com.mx",
                # "host": "localhost",
                "port": 5435
            }
            conexion_POST = psycopg2.connect(**credenciales)    
            cursor_POST = conexion_POST.cursor()

            # print("OK! conexión exitosa - POSTGRESQL")
            semestrenum_= 0
            semestredesc_ = ""
            select_ = "select amaiid  from tamateriaimpartir inner join taconfiguracion on tamateriaimpartir.amaianio=taconfiguracion.aconanio and tamateriaimpartir.amaiperiodo = taconfiguracion.aconperiodo;"
            
            #AMAIId de periodo:        
            cursor_POST.execute(select_)
            consulta_ = cursor_POST.fetchall()
            if len(consulta_) >  0:
                amaiid = consulta_[0][0]

                #Grado y grupo:
                select_ = "select tagrupo.agruid, agraid, aluid, amatclave, amaiid, tagrupo.semestreid, tasemestre.semestredesc, tasemestre.semestrenum from tagrupotagrupoalumnos inner join tagrupo on tagrupotagrupoalumnos.agruid = tagrupo.agruid inner join tasemestre on tasemestre.semestreid = tagrupo.semestreid where aluid=" + str(aluid_).strip() + " and amaiid=" + str(amaiid).strip() + " and tasemestre.semestrenum != 99 limit 1;"
                cursor_POST.execute(select_)
                consulta_ = cursor_POST.fetchall()
                if len(consulta_) >  0:
                    semestredesc_ = consulta_[0][6]
                    semestrenum_ = consulta_[0][7]

            cursor_POST.close()
            conexion_POST.close()

            return semestredesc_, semestrenum_
        except psycopg2.Error as e:
            print("Ocurrió un error al conectar a PostgreSQL: ", e)

def UpdateGrupoSemestre(cursorSql):
    ListaAlumnos = AlumnosBachillerV3(cursorSql)
    
    for item in ListaAlumnos:
        AL_Matricula = item[1]
        semestrenum = 0
        semetresdesc = ""
        semetresdesc, semestrenum = GrupoBachiller(AL_Matricula)

        if semestrenum != 0:
            update_GrupoSemestre = "update alumno set AL_Semestre ="+ str(semestrenum).strip() +", AL_Grupo = '" + str(semetresdesc).strip() + "' where AL_Matricula = '" + str(AL_Matricula).strip() + "';"
            cursorSql.execute(update_GrupoSemestre)
            cursorSql.commit()

            print("Alumno ", item[1], "actualizado al grupo ", semestrenum, semetresdesc)
###################################################################3
####Main__ 
conexion_SQL = conectar_SQL()

# Cursor SQL para tener la información que se insertará
with conexion_SQL.cursor() as cursor_sql:

    print(UpdateGrupoSemestre(cursor_sql))
                    
cursor_sql.close()