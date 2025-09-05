import mysql.connector
import csv
from datetime import datetime


def conectar_bd(database):
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="", 
            database=database
        )
    except mysql.connector.Error as error:
        print(f"Error al conectar a MySQL: {error}")
        return None

def cargar_csv(archivo):
    datos = []
    try:
        with open(archivo, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                datos.append(row)
        return datos
    except Exception as e:
        print(f"Error al leer el archivo CSV {archivo}: {e}")
        return None

def convertir_fecha(fecha_str):
    try:
        fecha_obj = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M')
        return fecha_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Error al convertir fecha {fecha_str}: {e}")
        return None

def insertar_usuarios(conexion, datos):
    cursor = conexion.cursor()
    
    try:
        # Procesar en lotes de 100 registros
        batch_size = 100
        for i in range(0, len(datos), batch_size):
            batch = datos[i:i+batch_size]
            
            for usuario in batch:
                fecha = convertir_fecha(usuario['fecha_creacion'])
                
                cursor.callproc('sp_insertUsuariosFromCSV_dbo_26823', [
                    usuario['userId'],
                    usuario['username'],
                    usuario['first_name'],
                    usuario['last_name'],
                    usuario['email'],
                    usuario['password_hash'],
                    usuario['rol'],
                    fecha
                ])
        
        conexion.commit()
        print(f"Se insertaron {len(datos)} registros en la tabla Usuarios usando procedimiento almacenado")
    except mysql.connector.Error as error:
        print(f"Error al insertar usuarios: {error}")
        conexion.rollback()
    finally:
        cursor.close()

def insertar_clientes(conexion, datos):
    cursor = conexion.cursor()
    
    try:
        for cliente in datos:
            fecha = convertir_fecha(cliente['fecha_registro'])
            
            # Llamar al procedimiento almacenado
            cursor.callproc('sp_insertClientesFromCSV_crm_26823', [
                cliente['cliente_id'],
                cliente['nombre'],
                cliente['apellido'],
                cliente['email'],
                fecha
            ])
        
        conexion.commit()
        print(f"Se insertaron {len(datos)} registros en la tabla Clientes usando procedimiento almacenado")
    except mysql.connector.Error as error:
        print(f"Error al insertar clientes: {error}")
        conexion.rollback()
    finally:
        cursor.close()

def main():
    usuarios = cargar_csv('usuarios.csv')
    clientes = cargar_csv('clientes.csv')
    
    if not usuarios or not clientes:
        print("Error al cargar los archivos CSV")
        return

    conexion_dbo = conectar_bd('dbo')
    if conexion_dbo:
        insertar_usuarios(conexion_dbo, usuarios)
        conexion_dbo.close()

    conexion_crm = conectar_bd('crm')
    if conexion_crm:
        insertar_clientes(conexion_crm, clientes)
        conexion_crm.close()

if __name__ == "__main__":
    main()