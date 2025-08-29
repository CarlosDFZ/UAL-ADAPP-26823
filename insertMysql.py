import mysql.connector
import csv
from datetime import datetime

# Optimización 1: Se usa context manager para conexiones
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

# Optimización 2: Se usa inserción por lotes
def insertar_usuarios(conexion, datos):
    cursor = conexion.cursor()
    sql = """INSERT INTO Usuarios 
            (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    try:
        # Procesar en lotes de 100 registros
        batch_size = 100
        valores = []
        for usuario in datos:
            fecha = convertir_fecha(usuario['fecha_creacion'])
            valores.append((
                usuario['userId'],
                usuario['username'],
                usuario['first_name'],
                usuario['last_name'],
                usuario['email'],
                usuario['password_hash'],
                usuario['rol'],
                fecha
            ))
            if len(valores) >= batch_size:
                cursor.executemany(sql, valores)
                valores = []
                
        # Insertar registros restantes
        if valores:
            cursor.executemany(sql, valores)
        
        conexion.commit()
        print(f"Se insertaron {len(datos)} registros en la tabla Usuarios")
    except mysql.connector.Error as error:
        print(f"Error al insertar usuarios: {error}")
        conexion.rollback()
    finally:
        cursor.close()

def insertar_clientes(conexion, datos):
    cursor = conexion.cursor()
    sql = """INSERT INTO Clientes 
            (cliente_id, nombre, apellido, email, FechaRegistro)
            VALUES (%s, %s, %s, %s, %s)"""
    
    try:
        for cliente in datos:
            fecha = convertir_fecha(cliente['fecha_registro'])
            valores = (
                cliente['cliente_id'],
                cliente['nombre'],
                cliente['apellido'],
                cliente['email'],
                fecha
            )
            cursor.execute(sql, valores)
        
        conexion.commit()
        print(f"Se insertaron {len(datos)} registros en la tabla Clientes")
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