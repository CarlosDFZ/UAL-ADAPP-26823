from fuzz_functions import execute_dynamic_matching_weighted
import pandas as pd
import os
import mysql.connector
import csv
from datetime import datetime


EXPORT_FOLDER = "exportaciones"

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
                if 'score' in item:
                    item_filtrado['score'] = item['score']
                if 'nombre_completo' in item:
                    item_filtrado['nombre_completo'] = item['nombre_completo']
                print(item_filtrado)
            else:
                print(item)

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
        if 'score' in new_item:
            new_item['score'] = f"{new_item['score']}%"
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
        if 'score' in new_item:
            new_item['score'] = f"{new_item['score']}%"
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

def separar_matched_records(results, score_rec=97):  
    matched_records = []
    unmatched_records = []
    for item in results:
        if 'score' in item and isinstance(item['score'], (int, float)):
            if item['score'] >= score_rec:
                matched_records.append(item)
            else:
                unmatched_records.append(item)
        else:
            unmatched_records.append(item)
    return matched_records, unmatched_records



def conectar_mysql(database):
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database=database
        )
    except mysql.connector.Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None
    
    
def insertar_matched_a_mysql(matched_records, table_name="all_matched_records", database="dbo"):
    """
    Inserta registros matched en la tabla especificada con número de control y muestra TODOS los registros
    """
    if not matched_records:
        print(f"No hay registros matched para insertar en {table_name}.")
        return
    
    print(f"Insertando {len(matched_records)} registros matched en {table_name}...")
    
    conexion = conectar_mysql(database)
    if not conexion:
        print("Error: No se pudo conectar a la base de datos")
        return
    
    try:
        cursor = conexion.cursor()
        
        # 1. Eliminar tabla existente solo si es all_matched_records
        if table_name == "all_matched_records":
            try:
                cursor.callproc("sp_eliminar_tabla", (table_name,))
                conexion.commit()
            except:
                pass
        
        # 2. Crear nueva tabla si no existe - CON NÚMERO DE CONTROL Y TIMESTAMP
        columnas_def = """
            id INT AUTO_INCREMENT PRIMARY KEY,
            control_number VARCHAR(10) NOT NULL UNIQUE,
            source_first_name VARCHAR(255),
            source_last_name VARCHAR(255),
            source_email VARCHAR(255),
            match_score DECIMAL(5,2),
            source_full_name VARCHAR(255),
            match_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
        try:
            cursor.callproc("sp_crear_tabla", (table_name, columnas_def))
            conexion.commit()
        except:
            pass
        
        # 3. Obtener el último número de control para continuar la secuencia
        last_control_number = "DR0000"
        try:
            cursor.execute(f"SELECT control_number FROM {table_name} ORDER BY id DESC LIMIT 1")
            last_record = cursor.fetchone()
            if last_record:
                last_control_number = last_record[0]
        except:
            # Si la tabla está vacía o no existe, empezar desde DR0001
            pass
        
        # Extraer el número secuencial del último control_number
        if last_control_number.startswith('DR'):
            try:
                last_number = int(last_control_number[2:])
            except:
                last_number = 0
        else:
            last_number = 0
        
        # 4. Preparar datos para inserción - CON NÚMERO DE CONTROL
        columnas = [
            "control_number", "source_first_name", "source_last_name", "source_email",
            "match_score", "source_full_name"
        ]
        columnas_str = ", ".join(columnas)
        
        valores_sql_list = []
        for i, record in enumerate(matched_records):
            # Generar número de control secuencial
            control_number = f"DR{last_number + i + 1:04d}"
            
            # Mapear campos individuales
            source_first_name = record.get('first_name', '') or record.get('source_first_name', '')
            source_last_name = record.get('last_name', '') or record.get('source_last_name', '')
            source_email = record.get('email', '') or record.get('source_email', '')
            
            # Crear nombre completo combinando nombre y apellido
            source_full_name = f"{source_first_name} {source_last_name}".strip()
            if not source_full_name:  # Si ambos están vacíos
                source_full_name = None
            
            match_score = record.get('score', 0)
            
            valores = [
                control_number,
                source_first_name,
                source_last_name,
                source_email,
                match_score,
                source_full_name
            ]
            
            # Formatear valores para SQL
            valores_formateados = []
            for valor in valores:
                if valor is None or valor == '':
                    valores_formateados.append("NULL")
                else:
                    valor_escaped = str(valor).replace("'", "''")
                    valores_formateados.append(f"'{valor_escaped}'")
            
            valores_sql_list.append("(" + ",".join(valores_formateados) + ")")
        
        valores_sql = ",".join(valores_sql_list)
        
        # 5. Insertar usando stored procedure
        cursor.callproc("sp_insertar_registros", (table_name, columnas_str, valores_sql))
        conexion.commit()
        
        print(f"Se insertaron {len(matched_records)} registros en '{table_name}'")
        
        # MOSTRAR TODOS LOS REGISTROS INSERTADOS CON NÚMERO DE CONTROL
        cursor.execute(f"SELECT control_number, source_first_name, source_last_name, source_full_name, match_score, match_timestamp FROM {table_name}")
        all_data = cursor.fetchall()
        
        print(f"\n=== TODOS LOS {len(all_data)} REGISTROS EN '{table_name.upper()}' ===")
        print("Control No. | First Name | Last Name | Full Name | Score | Timestamp")
        print("-" * 90)
        
        for row in all_data:
            control_num, first_name, last_name, full_name, score, timestamp = row
            print(f"{control_num:10} | {first_name or 'N/A':10} | {last_name or 'N/A':10} | {full_name or 'N/A':15} | {score:>5}% | {timestamp}")
        
        print("-" * 90)
        print(f"Total: {len(all_data)} registros")
        
    except Exception as e:
        print(f"Error al insertar registros en {table_name}: {e}")
        import traceback
        traceback.print_exc()
        conexion.rollback()
    finally:
        cursor.close()
        conexion.close()

def importar_datos(archivo, db):
    conexion = conectar_mysql(db)
    if not conexion:
        return

    with open(archivo, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columnas = reader.fieldnames
        columnas_sql = [col.replace(' ', '_').replace('(', '').replace(')', '').replace('%', '') for col in columnas]

        cursor = conexion.cursor()
        tabla = "match_records"

        try:
            # Eliminar tabla existente
            cursor.callproc("sp_eliminar_tabla", (tabla,))
            conexion.commit()

            # Crear nueva tabla con control_number AL INICIO y match_timestamp al final
            columnas_def = "control_number VARCHAR(10) NOT NULL UNIQUE, "
            columnas_def += ", ".join([f"{col} VARCHAR(255)" for col in columnas_sql])
            # Agregar timestamp al final
            columnas_def += ", match_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            cursor.callproc("sp_crear_tabla", (tabla, columnas_def))
            conexion.commit()

            valores = []
            for row in reader:
                fila = [row[col] if row[col] is not None and row[col] != '' else None for col in columnas]
                valores.append(fila)

            # Obtener el último número de control para continuar la secuencia
            last_control_number = "DR0000"
            try:
                cursor.execute(f"SELECT control_number FROM {tabla} ORDER BY id DESC LIMIT 1")
                last_record = cursor.fetchone()
                if last_record:
                    last_control_number = last_record[0]
            except:
                pass

            # Extraer el número secuencial del último control_number
            if last_control_number.startswith('DR'):
                try:
                    last_number = int(last_control_number[2:])
                except:
                    last_number = 0
            else:
                last_number = 0

            valores_sql_list = []
            for i, fila in enumerate(valores):
                # Generar número de control secuencial
                control_number = f"DR{last_number + i + 1:04d}"
                
                # Formatear valores para SQL (control_number AL INICIO)
                valores_fila = [f"'{control_number}'"]  # Control number primero
                for valor in fila:
                    if valor is None:
                        valores_fila.append("NULL")
                    else:
                        valor_escaped = str(valor).replace("'", "''")
                        valores_fila.append(f"'{valor_escaped}'")
                # El timestamp se genera automáticamente, no necesita ser incluido
                
                valores_sql_list.append("(" + ",".join(valores_fila) + ")")
            
            valores_sql = ",".join(valores_sql_list)
            
            # Preparar los nombres de columnas para la inserción (control_number AL INICIO)
            columnas_sql_str = "control_number, " + ", ".join(columnas_sql)

            cursor.callproc("sp_insertar_registros", (tabla, columnas_sql_str, valores_sql))
            conexion.commit()

            print(f"{len(valores)} registros insertados en la tabla '{tabla}' de la base '{db}'.")

            while True:
                ver_registros = input("¿Quieres ver los registros que se acaban de insertar? (s/n): ").lower()
                if ver_registros in ["s", "n"]:
                    break
                else:
                    print("Opción no válida. Ingresa 's' o 'n'.")

            if ver_registros == "s":
                print(f"\n=== REGISTROS EN LA TABLA '{tabla}' ===")
                cursor.execute(f"SELECT * FROM {tabla}")
                registros = cursor.fetchall()
                
                if not registros:
                    print("No hay registros en la tabla.")
                else:
                    cursor.execute(f"DESCRIBE {tabla}")
                    column_names = [col[0] for col in cursor.fetchall()]
                    
                    print(" | ".join(column_names))
                    print("-" * (len(" | ".join(column_names)) + 10))
                    
                    for registro in registros:
                        print(" | ".join(str(val) if val is not None else "NULL" for val in registro))

        except mysql.connector.Error as e:
            print(f"Error al importar datos: {e}")
            conexion.rollback()
        except Exception as e:
            print(f"Error general: {e}")
            conexion.rollback()
        finally:
            cursor.close()
            conexion.close()


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

# Ejecutar matching
pesos_personalizados = {'first_name': 2, 'last_name': 3, 'email': 5}
resultados = execute_dynamic_matching_weighted(params_dict, score_cutoff=70, column_weights=pesos_personalizados)
matched, unmatched = separar_matched_records(resultados, score_rec=97)
print(f"Registros matched mayor a 97%: {len(matched)}")
print(f"Registros unmatched menor a 97%: {len(unmatched)}")

insertar_matched_a_mysql(matched)

# Selección del usuario
while True:
    tipo_datos = input("¿Qué datos quieres procesar? (matched/unmatched/todos): ").lower()
    if tipo_datos in ["matched", "unmatched", "todos"]:
        break
    else:
        print("Opción no válida. Por favor, elige 'matched', 'unmatched' o 'todos'.")

if tipo_datos == "matched":
    datos_a_procesar = matched
    sufijo_archivo = "_matched"
elif tipo_datos == "unmatched":
    datos_a_procesar = unmatched
    sufijo_archivo = "_unmatched"
else:
    datos_a_procesar = resultados
    sufijo_archivo = ""

while True:
    formato = input("¿Quieres un DataFrame o un diccionario? (df/dic): ")
    if formato in ["df", "dic"]:
        df_temp = pd.DataFrame(datos_a_procesar)
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
        
        nombre_base_csv = input("Nombre base del archivo CSV (Enter para 'resultados'): ").strip() or "resultados"
        nombre_csv = f"{nombre_base_csv}{sufijo_archivo}.csv"
        nombre_base_excel = input("Nombre base del archivo Excel (Enter para 'resultados'): ").strip() or "resultados"
        nombre_excel = f"{nombre_base_excel}{sufijo_archivo}.xlsx"

        dataFrameOrDict(datos_a_procesar, formato, num_filas, columnas_renombradas)
        guardar_csv(datos_a_procesar, nombre_csv, num_filas, columnas_renombradas)
        guardar_excel(datos_a_procesar, nombre_excel, num_filas, columnas_renombradas)

        if tipo_datos != "todos":
            exportar_otro = input(f"¿Quieres también exportar los datos {'unmatched' if tipo_datos == 'matched' else 'matched'}? (s/n): ").lower()
            if exportar_otro == "s":
                otros_datos = unmatched if tipo_datos == "matched" else matched
                otro_sufijo = "_unmatched" if tipo_datos == "matched" else "_matched"
                otro_nombre_csv = f"{nombre_base_csv}{otro_sufijo}.csv"
                otro_nombre_excel = f"{nombre_base_excel}{otro_sufijo}.xlsx"
                guardar_csv(otros_datos, otro_nombre_csv, num_filas, columnas_renombradas)
                guardar_excel(otros_datos, otro_nombre_excel, num_filas, columnas_renombradas)
        break
    else:
        print("Formato no válido. Por favor, elige 'df' o 'dic'.")


while True:
    importar = input("¿Quieres importar algún archivo CSV a la base de datos? (s/n): ").lower()
    if importar in ["s", "n"]:
        break
    else:
        print("Opción no válida. Ingresa 's' o 'n'.")

if importar == "s":
    ruta_csv = input("Ruta del archivo CSV a importar: ").strip()
    importar_datos(ruta_csv, "dbo") 