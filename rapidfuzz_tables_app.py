from fuzz_functions import execute_dynamic_matching
import pandas as pd
import os

def dataFrameOrDict(results, type='df', num_filas=None, columnas_renombradas=None):
    if not results:
        print("No hay resultados para mostrar.")
        return
    if num_filas:
        results = results[:num_filas]
    
    processed_results = []
    for item in results:
        new_item = item.copy()
        
        if 'score' in new_item:
            new_item['score'] = f"{new_item['score']}%"
        
        if 'first_name' in new_item and 'last_name' in new_item:
            new_item['nombre_completo'] = f"{new_item['first_name']} {new_item['last_name']}"
        elif 'first_name' in new_item:
            new_item['nombre_completo'] = new_item['first_name']
        elif 'last_name' in new_item:
            new_item['nombre_completo'] = new_item['last_name']
        
        processed_results.append(new_item)
    
    if type == "df":
        df = pd.DataFrame(processed_results)
        if columnas_renombradas:
            rename_dict = {orig: nuevo for orig, nuevo in columnas_renombradas}
            
            columnas_especiales = []
            score_in_renamed = any(orig == 'score' for orig, _ in columnas_renombradas)
            nombre_completo_in_renamed = any(orig == 'nombre_completo' for orig, _ in columnas_renombradas)
            
            if not score_in_renamed:
                columnas_especiales.append('score')
            if not nombre_completo_in_renamed and 'nombre_completo' in df.columns:
                columnas_especiales.append('nombre_completo')
            
            columnas_a_mostrar = [orig for orig, _ in columnas_renombradas] + columnas_especiales
            
            columnas_validas = [col for col in columnas_a_mostrar if col in df.columns]
            if not columnas_validas:
                print("Ninguna de las columnas seleccionadas existe en el DataFrame.")
                return
            
            df = df[columnas_validas]
            df = df.rename(columns=rename_dict)
        print(df)
    
    elif type == "dic":
        for item in processed_results:
            if columnas_renombradas:
                item_filtrado = {}
                for orig, nuevo in columnas_renombradas:
                    if orig in item:
                        item_filtrado[nuevo] = item[orig]
                # Aqui siempre se incluye el score y nombre completo
                if 'score' in item:
                    item_filtrado['score'] = item['score']
                if 'nombre_completo' in item:
                    item_filtrado['nombre_completo'] = item['nombre_completo']
                print(item_filtrado)
            else:
                print(item)

EXPORT_FOLDER = "exportaciones"

def guardar_carpeta():
    if not os.path.exists(EXPORT_FOLDER):
        os.makedirs(EXPORT_FOLDER)
        print(f"Carpeta '{EXPORT_FOLDER}' creada.")

def guardar_csv(results, nombre_archivo="resultados.csv", num_filas=None, columnas_renombradas=None):
    if not results:
        print("No hay resultados para guardar.")
        return
    guardar_carpeta()
    if num_filas:
        results = results[:num_filas]
    
    processed_results = []
    for item in results:
        new_item = item.copy()
        
        # Aqui hago que el score se ponga en porcentaje
        if 'score' in new_item:
            new_item['score'] = f"{new_item['score']}%"
        
        # Se combina el nombre y apellido si existen
        if 'first_name' in new_item and 'last_name' in new_item:
            new_item['nombre_completo'] = f"{new_item['first_name']} {new_item['last_name']}"
        elif 'first_name' in new_item:
            new_item['nombre_completo'] = new_item['first_name']
        elif 'last_name' in new_item:
            new_item['nombre_completo'] = new_item['last_name']
        
        processed_results.append(new_item)
    
    df = pd.DataFrame(processed_results)
    
    if columnas_renombradas:
        rename_dict = {orig: nuevo for orig, nuevo in columnas_renombradas}
        
        columnas_especiales = []
        score_in_renamed = any(orig == 'score' for orig, _ in columnas_renombradas)
        nombre_completo_in_renamed = any(orig == 'nombre_completo' for orig, _ in columnas_renombradas)
        
        if not score_in_renamed:
            columnas_especiales.append('score')
        if not nombre_completo_in_renamed and 'nombre_completo' in df.columns:
            columnas_especiales.append('nombre_completo')
        
        columnas_a_guardar = [orig for orig, _ in columnas_renombradas] + columnas_especiales
        
        columnas_validas = [col for col in columnas_a_guardar if col in df.columns]
        if not columnas_validas:
            print("Ninguna de las columnas seleccionadas existe en el DataFrame.")
            return
        
        df = df[columnas_validas]
        df = df.rename(columns=rename_dict)
    
    ruta = os.path.join(EXPORT_FOLDER, nombre_archivo)
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"Resultados guardados en '{ruta}'")

def guardar_excel(results, nombre_archivo="resultados.xlsx", num_filas=None, columnas_renombradas=None):
    if not results:
        print("No hay resultados para guardar.")
        return
    guardar_carpeta()
    if num_filas:
        results = results[:num_filas]
    
    processed_results = []
    for item in results:
        new_item = item.copy()
        
        # Aqui hago que el score se ponga en porcentaje
        if 'score' in new_item:
            new_item['score'] = f"{new_item['score']}%"
        
        # Se combina el nombre y apellido si existen
        if 'first_name' in new_item and 'last_name' in new_item:
            new_item['nombre_completo'] = f"{new_item['first_name']} {new_item['last_name']}"
        elif 'first_name' in new_item:
            new_item['nombre_completo'] = new_item['first_name']
        elif 'last_name' in new_item:
            new_item['nombre_completo'] = new_item['last_name']
        
        processed_results.append(new_item)
    
    df = pd.DataFrame(processed_results)
    
    if columnas_renombradas:
        rename_dict = {orig: nuevo for orig, nuevo in columnas_renombradas}
        
        columnas_especiales = []
        score_in_renamed = any(orig == 'score' for orig, _ in columnas_renombradas)
        nombre_completo_in_renamed = any(orig == 'nombre_completo' for orig, _ in columnas_renombradas)
        
        if not score_in_renamed:
            columnas_especiales.append('score')
        if not nombre_completo_in_renamed and 'nombre_completo' in df.columns:
            columnas_especiales.append('nombre_completo')
        
        columnas_a_guardar = [orig for orig, _ in columnas_renombradas] + columnas_especiales
        
        columnas_validas = [col for col in columnas_a_guardar if col in df.columns]
        if not columnas_validas:
            print("Ninguna de las columnas seleccionadas existe en el DataFrame.")
            return
        
        df = df[columnas_validas]
        df = df.rename(columns=rename_dict)
    
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
        df_temp = pd.DataFrame(resultados)
        print("\nColumnas disponibles (incluyendo nuevas columnas automáticas):")
        
        columnas_especiales = []
        if 'first_name' in df_temp.columns or 'last_name' in df_temp.columns:
            columnas_especiales.append('nombre_completo')
        if 'score' in df_temp.columns:
            columnas_especiales.append('score (%)')
        
        todas_columnas = list(df_temp.columns) + columnas_especiales
        
        for i, columna in enumerate(todas_columnas, 1):
            if columna in columnas_especiales:
                print(f"{i}. {columna} (automática)")
            else:
                print(f"{i}. {columna}")
        
        while True:
            seleccion = input("\nSelecciona los números de las columnas que deseas (separados por comas): ")
            if not seleccion.strip():
                print("Debes seleccionar al menos una columna. Por favor, elige los números correspondientes.")
                continue
            
            try:
                indices = [int(x.strip()) - 1 for x in seleccion.split(',')]
                columnas_seleccionadas = [todas_columnas[i] for i in indices if 0 <= i < len(todas_columnas)]
                
                if not columnas_seleccionadas:
                    print("No se seleccionaron columnas válidas. Por favor, intenta nuevamente.")
                    continue
                
                columnas_reales = []
                for col in columnas_seleccionadas:
                    if col == 'score (%)':
                        columnas_reales.append('score')
                    elif col == 'nombre_completo':
                        columnas_reales.append('nombre_completo')
                    else:
                        columnas_reales.append(col)
                
                break    
            except (ValueError, IndexError):
                print("Selección no válida. Por favor, introduce números separados por comas.")

        # Aqio se permite al usuario renombrar cada columna
        columnas_renombradas = []
        print("\nAhora puedes renombrar cada columna seleccionada:")
        for columna_real in columnas_reales:
            nombre_display = columna_real
            if columna_real == 'score':
                nombre_display = 'score (%)'
            
            nuevo_nombre = input(f"Renombrar '{nombre_display}' a: ").strip()
            if not nuevo_nombre:
                nuevo_nombre = nombre_display
            
            columnas_renombradas.append((columna_real, nuevo_nombre))

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
        
        dataFrameOrDict(resultados, formato, num_filas, columnas_renombradas)
        guardar_csv(resultados, nombre_csv, num_filas, columnas_renombradas)
        guardar_excel(resultados, nombre_excel, num_filas, columnas_renombradas)
        break
    else:
        print("Formato no válido. Por favor, elige 'df' o 'dic'.")