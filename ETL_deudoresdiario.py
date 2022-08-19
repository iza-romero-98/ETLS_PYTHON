import pyodbc
import psycopg2
#import json
#from time import time


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


def valida_registro(rdid, cursor_POST):
    consulta_ = "select * from rdeudores where rdid=" + str(rdid).strip() + ";"
    cursor_POST.execute(consulta_)
    rDeudoresDiario_ = cursor_POST.fetchall()
    vRDId = 0
    if len(rDeudoresDiario_) >  0:
        if str(rDeudoresDiario_[0][0]).strip() == 'None' or str(rDeudoresDiario_[0][0]).strip() == '' or str(rDeudoresDiario_[0][0]).strip() == 'NoneType':
            vRDId = 0
        else:
            vRDId = int(rDeudoresDiario_[0][0])

    return vRDId

def copiar_info_2(rdid):
    ####Main__ apartir de aquí corren los procesos 
    conexion_SQL = conectar_SQL()
    conexion_POST = conectar_POST()
    conexion_POST.autocommit = True
    cursor_POST = conexion_POST.cursor() #Cursos para ejecutar consultas

    #1. Cursor SQL para tener la información que se insertará: DEUDORES 
    with conexion_SQL.cursor() as cursor_sql:
        #start_time = time() #Medir tiempo
        select_ = "select * from [dbo].RDeudores where RD=" + str(rdid).strip()+ ";"
        
        cursor_sql.execute(select_)
        rdeudores = cursor_sql.fetchall() #Resutados

        for item in rdeudores:
            rdid_ = int(item[0])
            vRDId = valida_registro(rdid_,cursor_POST)

            if vRDId == 0:
                #2. Insertar el registro de Deudores
                select_rdd = "select * from [dbo].RDeudoresdiario where RDId=" + str(rdid_).strip() + ";"
                cursor_sql.execute(select_rdd)
                rDeduoresDiario = cursor_sql.fetchall()

                insert_ = "INSERT INTO public.rdeudores(rdid, rdfecreg, rdtimereg, rdtimefin) VALUES (" + str(rdid_).strip() + ",'" +  str(item[1]).strip() + "','" + str(item[2]).strip() + "','" + str(item[3]).strip() + "');"
                cursor_POST.execute(insert_)
                # cursor_POST.commit()
                        

                #3. Toda la información del registro de deudores diario:
                for item_DD in rDeduoresDiario:
                    print(str(item_DD[1]).strip())
                    
                    #Transformar estatus:
                    if int(item_DD[26]) == 21:
                        rdestatus = 3003 #Activo
                    elif int(item_DD[26]) == 22:
                        rdestatus = 55 #Inactivo
                    elif int(item_DD[26]) == 34:
                        rdestatus = 3478 #Preinscrito
                    elif int(item_DD[26]) == 35:
                        rdestatus = 3480 #Reinscrito
                    elif int(item_DD[26]) == 41:
                        rdestatus = 3328 #Baja por documentos
                    elif int(item_DD[26]) == 38:
                        rdestatus = 3479 #Inactivo

                    insert_ = "INSERT INTO public.rdeudoresdiario(rdaluid, rdpendiente, rddivision, rdapaterno, rdamaterno, rdnombre, rdemail, rdacarid,"
                    insert_ += "rdnocontrol, rdgrupo, rdgrado, rdemailt, rdnoconceptos,rdbcid, rdbeparcialidades, rdbeinscrip, rdbedesc, rdtutor,"
                    insert_ += "rdid, rdconvenio, rdconvinicio, rdconvfin, rdtelefono, rdestatus, rdconceptosdesc) VALUES ("
                    insert_ += str(item_DD[1]).strip() + "," + str(item_DD[2]).strip() + ",'" + str(item_DD[4]).strip() + "','" +  str(item_DD[5]).strip() + "','" 
                    insert_ += str(item_DD[6]).strip() + "','" + str(item_DD[7]).strip() + "','" + str(item_DD[8]).strip() + "','" 
                    insert_ += str(item_DD[10]).strip() + "','" + str(item_DD[11]).strip() + "','" +  str(item_DD[12]).strip() + "','" + str(item_DD[13]).strip() + "','" + str(item_DD[14]).strip() + "'," + str(item_DD[15]).strip() + ",'"
                    insert_ += str(item_DD[17]).strip() + "'," + str(item_DD[18]).strip() + "," + str(item_DD[19]).strip() + ",'" + str(item_DD[20]).strip() + "','" + str(item_DD[21]).strip() + "',"
                    insert_ += str(rdid_).strip() + "," + str(item_DD[22]).strip() + ",'" + str(item_DD[23]).strip() + "','" + str(item_DD[24]).strip() + "','" +  str(item_DD[25]).strip() + "'," 
                    insert_ += str(rdestatus).strip() + ",'" + str(item_DD[28]).strip() + "');"

                    print(insert_)
                    cursor_POST.execute(insert_)
                    # cursor_POST.commit()

    cursor_sql.close()
    cursor_POST.close()