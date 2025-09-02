from fuzz_functions import execute_dynamic_matching
import pandas as pd

def dataFrameOrDict(results, type='df'):
    if type == "df":
        df = pd.DataFrame(results)
        print(df)
    elif type == "dic":
        for item in results:
            print(item)

def guardar_csv(results, nombre_archivo="resultados.csv"):
    df = pd.DataFrame(results)
    df.to_csv(nombre_archivo, index=False, encoding="utf-8-sig")
    print(f"Resultados guardados en '{nombre_archivo}'")


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
        dataFrameOrDict(resultados, formato)
        guardar_csv(resultados)
        break
    else:
        print("Formato no válido. Por favor, elige 'df' o 'dic'.")
