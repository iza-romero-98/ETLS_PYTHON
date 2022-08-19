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
from ETL_deudoresdiario import * 
    

def conectar_SQL():
    # direccion_servidor = '217.160.15.235' 
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
            semestreid_ = 0
            tutor_ = ""
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
                    semestreid_ = consulta_[0][5]
                    semestredesc_ = consulta_[0][6]
                    semestrenum_ = consulta_[0][7]

                    #Tutor:
                    if semestreid_ != 0:
                        select_ = "select * from tatutorsemestre  inner join tatutor on tatutorsemestre.tclave=tatutor.tclave where tatutor.tclave != 'CY.CHAVEZ' and tatutor.tclave != 'CA.LOPEZ' and semestreid=" + str(semestreid_).strip() + ";"
                        cursor_POST.execute(select_)
                        consulta_ = cursor_POST.fetchall()
                        if len(consulta_) >  0:
                            tutor_ = consulta_[0][4]
                        
            
            cursor_POST.close()
            conexion_POST.close()

            return semestredesc_, semestrenum_,tutor_

        

        except psycopg2.Error as e:
            print("Ocurrió un error al conectar a PostgreSQL: ", e)



def RDeudores(cursor_sql):
     #************INSERTA EN RDEUDORES
    insert_deudores = "INSERT INTO [dbo].[RDeudores] ([RDFecreg]) VALUES('" + str(datetime.now().year).strip() 
    insert_deudores += "-" + str(datetime.now().month).strip() + "-" + str(datetime.now().day).strip() 
    insert_deudores += " " + str(datetime.now().hour).strip() + ":" + str(datetime.now().minute).strip() 
    insert_deudores += ":" + str(datetime.now().second).strip() + "." + str(datetime.now().microsecond*100)[0:3] + "');"

    cursor_sql.execute(insert_deudores)
    cursor_sql.commit()

    #************ULTIMO ID RDEUDORES
    select_RD = "SELECT TOP (1) [RD] FROM [dbo].[RDeudores] order by rd desc;"
    cursor_sql.execute(select_RD)
    RD = cursor_sql.fetchall() #Resutados

    for rd_ in RD:
        RDId_ =  int(rd_[0])
    
    return RDId_

def CierreProceso(cursor_sql, RDId_):
    update_ = "update [dbo].[RDeudores] set RDTimefin='" + str(datetime.now().year).strip() + "-" + str(datetime.now().month).strip() + "-" + str(datetime.now().day).strip() + " " + str(datetime.now().hour).strip() + ":" + str(datetime.now().minute).strip() + ":" + str(datetime.now().second).strip() + "." + str(datetime.now().microsecond*100)[0:3]  + "' where rd=" + str(RDId_).strip() + ";"
    
    cursor_sql.execute(update_)
    cursor_sql.commit()


def NotificacionAdeudo(correo):
    # import necessary packages
    locale.setlocale(locale.LC_ALL, 'es-ES') 
    
    # create message object instance
    msg = MIMEMultipart()
    
    message = "<p class='MsoNormal'><strong>Estimado Padre de Familia y/o Alumno (a),</strong></p><p class='MsoNormal'></p>"
    message += "<p class='MsoNormal'>Su fecha límite de pago ha vencido lo invitamos a cubrir sus pagos atrasados. Los montos pendientes <strong>vencidos</strong> al día de hoy <strong>" 
    message += str(datetime.now().day) + " de " + datetime.now().strftime("%B") + "</strong> ya cuentan con recargo este no se condona, no se aplaza y mucho menos se cancela.</p><p class='MsoNormal'>"
    message += " Le recordamos que las fechas de vencimiento de colegiaturas, bajo reglamento, son los primeros <strong>CINCO DÍAS DEL MES.</strong> Lo invitamos a cumplir con el reglamento y liquidar sus "
    message += " pagos atrasados para evitar cualquier inconveniente con el acceso a la <strong>plataforma estudiantil</strong>.</p><p class='MsoNormal'></p><p class='MsoNormal'>"
    message += " Para consultar sus conceptos, fechas de vencimiento o realizar un pago, ingrese al sistema estudiantil en el perfil de alumno en la sección de pagos.</p><p class='MsoNormal'></p>"
    message += " <p class='MsoNormal'><strong>IMPORTANTE: Los pagos que se realizan en sucursales de OXXO se ven reflejados de 24 a 48 hrs.</strong></p><p class='MsoNormal'></p><p class='MsoNormal'>"
    message += " <strong>ATENTAMENTE</strong></p><p class='MsoNormal'><strong>DEPTO. DE PAGOS DE LA UNIVERSIDAD INTERAMERICANA.</strong></p> <p class='MsoNormal'><strong>*ESTE CORREO ES ENVIADO DE MANERA "
    message += " AUTOMÁTICA, NO ES NECESARIO RESPONDER*"
    
    # setup the parameters of the message
    password = "inter2022"
    msg['From'] = "notificaciones@lainter.edu.mx"
    msg['To'] = correo
    msg['Subject'] = "***ATENTA INVITACIÓN FAVOR DE PASAR A LIQUIDAR IMPORTE PENDIENTE***"
    
    # add in the message body
    msg.attach(MIMEText(message, 'html'))
    
    #create server
    server = smtplib.SMTP('smtp.gmail.com: 587')
    
    server.starttls()

    # Login Credentials for sending the mail
    server.login(msg['From'], password)
    
    
    # send the message via the server.
    server.sendmail(msg['From'], msg['To'], msg.as_string())    
    server.quit()


def AlumnosDeudores(RDID_,cursor_sql):
    #Determinar fechas por "periodo":
    if datetime.now().month <= 7:
        FInicio_ = str(datetime.now().year).strip() + "-01-01"
    else:
        FInicio_ = str(datetime.now().year).strip() + "-08-01"

    select_  = "select distinct(CPC_AlumnoID) from DetalleCuentaPorCobrar "
    select_ += "inner join CuentasPorCobrar on DetalleCuentaPorCobrar.DCPC_CuentaId=CuentasPorCobrar.CPC_ID "
    select_ += "inner join Alumno on CuentasPorCobrar.CPC_AlumnoID = Alumno.AL_ID "
    select_ += "where AL_StatusActual in (34, 35, 38, 41, 21, 22) and DCPC_ImportePendiente <> 0 and DCPC_Estatus=21 and ("
    select_ +=  "DCPC_FechaVencimiento>'" + FInicio_ + "' and DCPC_FechaVencimiento < GETDATE()); "

    cursor_sql.execute(select_)
    alumnos_ = cursor_sql.fetchall() #Resutados

    for item in alumnos_:
        if item[0] > 0:
            # print(item[0])
            
            select_cuentas = "select DCPC_ID, DCPC_ImportePendiente, DCPC_Descripcion from DetalleCuentaPorCobrar "
            select_cuentas += "inner join CuentasPorCobrar on DetalleCuentaPorCobrar.DCPC_CuentaId=CuentasPorCobrar.CPC_ID where DCPC_ImportePendiente <> 0 and DCPC_Estatus=21 and ("
            select_cuentas += "DCPC_FechaVencimiento>'" + FInicio_ + "' and DCPC_FechaVencimiento < GETDATE()) and CPC_AlumnoID=" + str(item[0]).strip() + ";"


            cursor_sql.execute(select_cuentas)
            cuentas_alumno = cursor_sql.fetchall() #Resutados

            adeudo_alumno = 0
            noconceptos_ = 0 
            descripcion_ = ""
            for cuenta in cuentas_alumno:
                # print(cuenta[0])               

                if cuenta[1] > 0:
                    select_convenios = "select count(*) from DetalleConvenio inner join Convenios on DetalleConvenio.DC_Convenio=Convenios.Con_ID "
                    select_convenios += " where DC_CuentaDetalle=" + str(cuenta[0]).strip() + " and Con_Estatus=27;"

                    cursor_sql.execute(select_convenios)
                    convenios_cuenta = cursor_sql.fetchall() #Resultados
                    if len(convenios_cuenta) > 0 and convenios_cuenta[0][0] == 0:
                        noconceptos_ += 1
                        descripcion_ += str(cuenta[2]).strip() + " // "
                        adeudo_alumno += cuenta[1]
                else:
                    adeudo_alumno += cuenta[1]
            

            if adeudo_alumno > 0:
                AlumnoDeudor(item[0],adeudo_alumno, noconceptos_, descripcion_[0:len(descripcion_) - 3], RDID_, cursor_sql)
                # print(item[0], " insertar ")       

def AlumnoDeudor(AL_Id, RDPedindiente, RDNoConceptos, RDConceptosdesc, RDID_, cursor_sql):

    select_DG = "select Alumno.AL_ID, AL_Matricula, AL_Nombre, AL_APP, AL_APM, GA_NombreTutor, GA_APPTutor, GA_APMTutor, GA_TelefonoTutor,"
    select_DG += "GA_TelefonoAlumno, GA_CorreoAlternativo, GA_TelefonoCasa, Carreraclave, Nivel_Nombre, AL_StatusActual, SL_Descripcion, AL_Carrera, "
    select_DG += "Carreras.Nivel_ID, AL_CorreoInst "
    select_DG += "from Alumno inner join GeneralesAlumno on Alumno.AL_ID = GeneralesAlumno.AL_ID "
    select_DG += "inner join Carreras on Alumno.AL_Carrera = Carreras.IDCarrera "
    select_DG += "inner join Niveles on Carreras.Nivel_ID = Niveles.Nivel_ID "
    select_DG += "inner join EstatusList on Alumno.AL_StatusActual = EstatusList.SL_StatusID where Alumno.AL_ID=" + str(AL_Id).strip() + ";"

    cursor_sql.execute(select_DG)
    datosgenerales_ = cursor_sql.fetchall()

        # 0 al_id 
        # 1 AL_Matricula, 
        # 2 AL_Nombre, 
        # 3 AL_APP, 
        # 4 AL_APM, 
        # 5 GA_NombreTutor, 
        # 6 GA_APPTutor, 
        # 7 GA_APMTutor, 
        # 8 GA_TelefonoTutor, 
        # 9 GA_TelefonoAlumno, 
        # 10 GA_CorreoAlternativo, 
        # 11 GA_TelefonoCasa, 
        # 12 Carreraclave, 
        # 13 Nivel_Nombre 
        # 14 AL_StatusActual
        # 15 SL_Descripcion
        # 16 AL_Carrera 
        # 17 RDDivision
        # 18 AL_CorreoInst

    if len(datosgenerales_) > 0:
        al_id = datosgenerales_[0][0]
        AL_Matricula = str(datosgenerales_[0][1]).strip()
        AL_Nombre = str(datosgenerales_[0][2]).strip()
        AL_APP = str(datosgenerales_[0][3]).strip()
        AL_APM = str(datosgenerales_[0][4]).strip()
        # GA_NombreTutor = str(datosgenerales_[0][5]).strip()
        # GA_APPTutor = str(datosgenerales_[0][6]).strip()
        # GA_APMTutor = str(datosgenerales_[0][7]).strip()
        GA_TelefonoTutor = str(datosgenerales_[0][8]).strip()
        GA_TelefonoAlumno = str(datosgenerales_[0][9]).strip()
        GA_CorreoAlternativo = str(datosgenerales_[0][10]).strip()
        GA_TelefonoCasa = str(datosgenerales_[0][11]).strip()
        Carreraclave = str(datosgenerales_[0][12]).strip()
        Nivel_Nombre = str(datosgenerales_[0][13]).strip()
        AL_StatusActual = str(datosgenerales_[0][14]).strip()
        SL_Descripcion = str(datosgenerales_[0][15]).strip()
        AL_Carrera = str(datosgenerales_[0][16]).strip()
        RDDivision = str(datosgenerales_[0][17]).strip()
        RDEmail = str(datosgenerales_[0][18]).strip()

        informacion_beca = "select beinscrip, beparcialidades, con_Descripcion, Con_Clave from becaAlumno "
        informacion_beca += "inner join CatalogoConceptos on becaAlumno.conceptoid = CatalogoConceptos.Con_ID "
        informacion_beca += "where AL_ID=" + str(al_id).strip()+ " and beperiodo=" + "1" if datetime.now().month < 8 else "3"
        informacion_beca += " and beanio=" + str(datetime.now().year).strip() + ";" 
        # " and beestatus = 'ACT';" 
        
        cursor_sql.execute(informacion_beca)
        BecaAlumno = cursor_sql.fetchall()
        if len(BecaAlumno) > 0:
            RDBcid_ = BecaAlumno[0][3]
            Beca_ = BecaAlumno[0][2]
            RDBEParcialidades_ = BecaAlumno[0][1]
            RDBeinscrip_ = BecaAlumno[0][0]
        else:
            RDBcid_ = 0
            Beca_ = ""
            RDBEParcialidades_ = 0
            RDBeinscrip_ = 0

        
        #Información convenios:
        # info_convenio = "select TOP(1) Con_FechaInicio, Con_FechaFin from Convenios where con_estatus=27 "
        # info_convenio += "and (Getdate()>=Con_FechaInicio and GETDATE() <= Con_FechaFin) and Con_Alid=" +str(al_id).strip() + " order by con_Id desc;"

        # cursor_sql.execute(info_convenio)
        # convenio_ = cursor_sql.fetchall()
        RDConvinicio = ""
        RDConvfin = ""
        RDConvenio = 0

        # if len(convenio_) > 0:
        #     RDConvenio = 1
        #     RDConvinicio = convenio_[0][0]
        #     RDConvfin = convenio_[0][1]      

        
        semestrenum = 0
        semetresdesc = ""
        GA_NombreTutor = ""
        if Nivel_Nombre == 'BAC':
            semetresdesc, semestrenum, GA_NombreTutor = GrupoBachiller(AL_Matricula)
        else:
            GA_NombreTutor = ""

        insert_deudor = "INSERT INTO [dbo].[RDeudoresdiario] ([RDId], [RDAluid],[RDPedindiente],[RDDivision],[Div],[RDAPaterno],[RDAMaterno]"
        insert_deudor += ",[RDNombre],[RDEmail],[RDCARID],[Carrera],[RDNOControl],[RDEmailT],[RDNoConceptos],[RDBcid],[Beca],[RDBEParcialidades]"
        insert_deudor += ",[RDBeinscrip],[RDBedesc],[RDTutor],[RDTelefono],[RDEstatus],[Estatus],[RDConceptosdesc],[RDGrado],[RDGrupo],[RDConvenio],[RDConvinicio],[RDConvfin])"
        insert_deudor += " VALUES (" + str(RDID_).strip() + "," + str(al_id).strip() + "," + str(RDPedindiente).strip() + "," + str(RDDivision).strip() + ",'" + str(Nivel_Nombre).strip() + "','"
        insert_deudor += str(AL_APP).strip() + "','" + str(AL_APM).strip() + "','" + str(AL_Nombre).strip() + "','" + str(RDEmail).strip() + "',"
        insert_deudor += str(AL_Carrera).strip() + ",'" + str(Carreraclave).strip() + "','" + str(AL_Matricula).strip() + "','" + str(GA_CorreoAlternativo).strip()+ "',"
        insert_deudor += str(RDNoConceptos).strip() + "," + str(RDBcid_).strip() + ",'" + str(Beca_).strip() + "'," + str(RDBEParcialidades_).strip() + "," + str(RDBeinscrip_).strip() + ",'"
        insert_deudor += str(Beca_).strip() + "','" + str(GA_NombreTutor).strip() + "','" + str(GA_TelefonoTutor).strip() + "'," + str(AL_StatusActual).strip() + ",'" + str(SL_Descripcion).strip() + "','" + str(RDConceptosdesc).strip() + "','"
        insert_deudor += str(semestrenum).strip() + "','" + str(semetresdesc).strip() + "'," + str(RDConvenio).strip() + ",'" +  str(RDConvinicio).strip() + "','" + str(RDConvfin).strip() + "');"

        # print(insert_deudor)  
        cursor_sql.execute(insert_deudor)
        cursor_sql.commit()

        #Enviar notificacion 
        if AL_StatusActual != "22":
            print("alumno deudor ", AL_Matricula, " - ", str(RDPedindiente).strip())
            # NotificacionAdeudo(GA_CorreoAlternativo)

###################################################################3
####Main__ 
conexion_SQL = conectar_SQL()

# Cursor SQL para tener la información que se insertará
with conexion_SQL.cursor() as cursor_sql:

    RDId_ = RDeudores(cursor_sql)
    AlumnosDeudores(RDId_, cursor_sql)
    CierreProceso(cursor_sql, RDId_)

    copiar_info_2(RDId_)
                    
cursor_sql.close()