from fuzz_functions import execute_dynamic_matching
import pandas as pd
import os

def dataFrameOrDict(results, type='df', num_filas=None):
    if not results:
        print("No hay resultados para mostrar.")
    if num_filas:
        results = results[:num_filas]
    if type == "df":
        df = pd.DataFrame(results)
        print(df)
    elif type == "dic":
        for item in results:
            print(item)
            
EXPORT_FOLDER = "exportaciones"

def guardar_carpeta():
    if not os.path.exists(EXPORT_FOLDER):
        os.makedirs(EXPORT_FOLDER)
        print(f"Carpeta '{EXPORT_FOLDER}' creada.")

def guardar_csv(results, nombre_archivo="resultados.csv", num_filas=None):
    if not results:
        print("No hay resultados para guardar.")
    guardar_carpeta()
    if num_filas:
        results = results[:num_filas]
    df = pd.DataFrame(results)
    ruta = os.path.join(EXPORT_FOLDER, nombre_archivo)
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"Resultados guardados en '{ruta}'")
    
def guardar_excel(results, nombre_archivo="resultados.xlsx", num_filas=None):
    if not results:
        print("No hay resultados para guardar.")
    guardar_carpeta()
    if num_filas:
        results = results[:num_filas]
    df = pd.DataFrame(results)
    ruta = os.path.join(EXPORT_FOLDER, nombre_archivo)
    df.to_excel(ruta, index=False)
    print(f"Resultados guardados en '{ruta}'")


params_dict = {
    "server": "localhost",
    "database": "dbo",
    "username": "root",
    "password": "",
    "sourceSchema": "dbo",
    "sourceTable": "Usuarios",
    "destSchema": "crm",
    "destTable": "Clientes",
    "src_dest_mappings": {
        "first_name": "nombre",
        "last_name": "apellido",
        "email": "email"
    }
}

# Se cambió el score_cutoff a 70
resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

while True:
    formato = input("¿Quieres un DataFrame o un diccionario? (df/dic): ")
    if formato in ["df", "dic"]:
        while True:
            num_filas = input("¿Cuántas filas quieres mostrar? (Enter para todas): ")
            if num_filas.strip():
                try:
                    num_filas = int(num_filas)
                    if num_filas == 0:
                        print("Error: El número de filas no puede ser 0. Por favor, introduce otro número.")
                        continue
                    break
                except ValueError:
                    print("Error: Por favor, introduce un número válido.")
                    continue
            else:
                num_filas = None
                break
        
        nombre_csv = input("Nombre del archivo CSV (Enter para 'resultados.csv'): ")
        nombre_csv = nombre_csv if nombre_csv.strip() else "resultados.csv"
        
        nombre_excel = input("Nombre del archivo Excel (Enter para 'resultados.xlsx'): ")
        nombre_excel = nombre_excel if nombre_excel.strip() else "resultados.xlsx"
        
        dataFrameOrDict(resultados, formato, num_filas)
        guardar_csv(resultados, nombre_csv, num_filas)
        guardar_excel(resultados, nombre_excel, num_filas)
        break
    else:
        print("Formato no válido. Por favor, elige 'df' o 'dic'.")
