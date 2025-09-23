from fuzz_functions import execute_dynamic_matching_weighted
from manejo_peso import obtener_pesos_columnas, actualizar_peso_columna
from sincronizar_peso import cargar_pesos_desde_config
from rapidfuzz import fuzz
import pandas as pd
import os
import mysql.connector
import csv
import subprocess
from datetime import datetime
from decimal import Decimal


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


def inicializar_sistema():
    def simular_matching(pesos):
        """Realiza una simulación del matching con los pesos proporcionados"""
        print("\n=== SIMULACIÓN DE MATCHING CON PESOS ACTUALES ===")
        
        # Convertir pesos a float si son Decimal
        pesos_float = {k: float(v) for k, v in pesos.items()}
        
        try:
            # Conectar a las bases de datos
            conn_dbo = conectar_mysql('dbo')
            conn_crm = conectar_mysql('crm')
            
            if not conn_dbo or not conn_crm:
                print("Error: No se pudo conectar a las bases de datos")
                return
                
            cursor_dbo = conn_dbo.cursor(dictionary=True)
            cursor_crm = conn_crm.cursor(dictionary=True)
            
            # Obtener 2 registros de ejemplo de cada tabla
            cursor_dbo.execute("""
                SELECT first_name, last_name, email 
                FROM Usuarios 
                LIMIT 2
            """)
            usuarios = cursor_dbo.fetchall()
            
            cursor_crm.execute("""
                SELECT nombre, apellido, email 
                FROM Clientes 
                LIMIT 100
            """)
            clientes = cursor_crm.fetchall()
            
            print("\nRegistros de ejemplo para simulación:")
            print("\nUsuarios (origen):")
            for i, usuario in enumerate(usuarios, 1):
                print(f"\nUsuario {i}:")
                print(f"  Nombre: {usuario['first_name']}")
                print(f"  Apellido: {usuario['last_name']}")
                print(f"  Email: {usuario['email']}")
            
            print("\nResultados del matching:")
            print("=" * 80)
            
            for usuario in usuarios:
                print(f"\nBuscando coincidencias para: {usuario['first_name']} {usuario['last_name']}")
                print("-" * 60)
                
                matches = []
                
                for cliente in clientes:
                    # Calcular score por cada campo
                    email_score = fuzz.ratio(usuario['email'], cliente['email']) * (pesos_float['email'] / 10)
                    nombre_score = fuzz.ratio(usuario['first_name'], cliente['nombre']) * (pesos_float['first_name'] / 10)
                    apellido_score = fuzz.ratio(usuario['last_name'], cliente['apellido']) * (pesos_float['last_name'] / 10)
                    
                    # Calcular score total ponderado
                    total_weight = sum(pesos_float.values())
                    score_total = (email_score + nombre_score + apellido_score) / (total_weight / 10)
                    
                    # Almacenar coincidencia
                    matches.append({
                        'cliente': cliente,
                        'score': score_total,
                        'desglose': {
                            'email': email_score,
                            'nombre': nombre_score,
                            'apellido': apellido_score
                        }
                    })
                
                # Ordenar matches por score de mayor a menor y tomar los 5 mejores
                matches = sorted(matches, key=lambda x: x['score'], reverse=True)[:5]
                
                if matches and matches[0]['score'] >= 70:
                    print("\nTop 5 coincidencias encontradas:")
                    for i, match in enumerate(matches, 1):
                        print(f"\n{i}. Score: {match['score']:.2f}%")
                        print(f"   Cliente: {match['cliente']['nombre']} {match['cliente']['apellido']}")
                        print(f"   Email: {match['cliente']['email']}")
                        print("   Desglose de puntuación:")
                        print(f"   - Email: {match['desglose']['email']:.2f}%")
                        print(f"   - Nombre: {match['desglose']['nombre']:.2f}%")
                        print(f"   - Apellido: {match['desglose']['apellido']:.2f}%")
                else:
                    print("No se encontraron coincidencias significativas (umbral: 70%)")
            
            print("\n" + "=" * 80)
            
        except Exception as e:
            print(f"Error durante la simulación: {e}")
        finally:
            if 'conn_dbo' in locals() and conn_dbo:
                conn_dbo.close()
            if 'conn_crm' in locals() and conn_crm:
                conn_crm.close()
    
    def mostrar_pesos():
        print("\n=== SISTEMA DE GESTIÓN DE PESOS ===")
        
        # Obtener y mostrar pesos de la base de datos
        db_weights, db_timestamp = obtener_pesos_columnas()
        print("\nPesos en la base de datos:")
        if db_weights:
            for column, weight in db_weights.items():
                print(f"- {column}: {weight:.2f}")
            print(f"Última actualización: {db_timestamp}")
            # Realizar simulación con los pesos de la base de datos
            simular_matching(db_weights)
        else:
            print("No se pudieron obtener los pesos de la base de datos")

        # Obtener y mostrar pesos del archivo config
        config_weights, config_timestamp = cargar_pesos_desde_config()
        print("\nPesos en archivo config.py:")
        if config_weights:
            for column, weight in config_weights.items():
                print(f"- {column}: {weight:.2f}")
            print(f"Última actualización: {config_timestamp}")
            if config_weights != db_weights:
                print("\nLos pesos en config.py son diferentes, simulando con estos pesos:")
                simular_matching(config_weights)
        else:
            print("No se pudieron obtener los pesos del archivo config.py")
        
        return db_weights, db_timestamp, config_weights, config_timestamp
        
    def mostrar_historial():
        """Muestra el historial de cambios de pesos"""
        try:
            conn = conectar_mysql('dbo')
            if not conn:
                print("Error: No se pudo conectar a la base de datos")
                return
            
            cursor = conn.cursor(dictionary=True)
            
            # Verificar si existe la tabla
            cursor.execute("""
                SELECT COUNT(*) as count FROM information_schema.tables 
                WHERE table_schema = 'dbo' AND table_name = 'peso_historial'
            """)
            if cursor.fetchone()['count'] == 0:
                print("\nNo existe la tabla de historial. Creando...")
                with open('peso_historial.sql', 'r', encoding='utf-8') as file:
                    sql_commands = file.read().split(';')
                    for command in sql_commands:
                        if command.strip():
                            cursor.execute(command)
                conn.commit()
                print("Tabla de historial creada correctamente")
                print("No hay registros en el historial todavía")
                return
                
            # Mostrar opciones de columnas
            print("\n=== COLUMNAS DISPONIBLES ===")
            columnas_disponibles = [
                ('1', 'ID'),
                ('2', 'Columna'),
                ('3', 'Anterior'),
                ('4', 'Nuevo'),
                ('5', 'Fecha/Hora'),
                ('6', 'Usuario'),
                ('7', 'Motivo')
            ]
            
            for num, col in columnas_disponibles:
                print(f"{num}. {col}")
            
            # Solicitar selección de columnas
            while True:
                seleccion = input("\nSeleccione los números de las columnas a mostrar (separados por comas) o Enter para todas: ").strip()
                if not seleccion:
                    columnas_seleccionadas = [col[1] for col in columnas_disponibles]
                    break
                try:
                    nums_seleccionados = [int(n.strip()) for n in seleccion.split(',')]
                    columnas_seleccionadas = [col[1] for i, col in enumerate(columnas_disponibles, 1) 
                                            if i in nums_seleccionados]
                    if columnas_seleccionadas:
                        break
                    else:
                        print("Debe seleccionar al menos una columna válida")
                except ValueError:
                    print("Por favor, ingrese números válidos separados por comas")
            
            # Preguntar si desea orden ascendente o descendente
            while True:
                orden = input("\n¿Cómo desea ordenar los registros? (asc/desc): ").lower()
                if orden in ['asc', 'desc']:
                    break
                print("Por favor, ingrese 'asc' para ascendente o 'desc' para descendente")

            # Obtener registros del historial con el orden especificado
            cursor.execute(f"""
                SELECT column_name, peso_anterior, peso_nuevo, 
                       fecha_cambio, usuario, motivo
                FROM peso_historial 
                ORDER BY fecha_cambio {' DESC' if orden == 'desc' else ' ASC'} 
                LIMIT 20
            """)
            registros = cursor.fetchall()
            
            if not registros:
                print("\nNo hay registros en el historial")
                return
            
            print("\n=== HISTORIAL DE CAMBIOS DE PESOS ===")
            print("=" * 120)
            
            # Construir el encabezado basado en las columnas seleccionadas
            headers = []
            widths = {
                'ID': 4,
                'Columna': 15,
                'Anterior': 10,
                'Nuevo': 10,
                'Fecha/Hora': 20,
                'Usuario': 15,
                'Motivo': 25
            }
            
            header_str = ""
            separator = ""
            
            for col in columnas_seleccionadas:
                width = widths[col]
                header_str += f"{col:<{width}} "
                separator += "=" * width + " "
            
            print(header_str)
            print(separator)
            
            # Mostrar los registros con las columnas seleccionadas
            for i, reg in enumerate(registros, 1):
                fecha_str = reg['fecha_cambio'].strftime('%Y-%m-%d %H:%M:%S')
                motivo = reg['motivo'][:24] + '...' if reg['motivo'] and len(reg['motivo']) > 24 else reg['motivo'] or 'N/A'
                
                row_str = ""
                for col in columnas_seleccionadas:
                    width = widths[col]
                    if col == 'ID':
                        row_str += f"{i:<{width}} "
                    elif col == 'Columna':
                        row_str += f"{reg['column_name']:<{width}} "
                    elif col == 'Anterior':
                        row_str += f"{reg['peso_anterior']:<{width}.2f} "
                    elif col == 'Nuevo':
                        row_str += f"{reg['peso_nuevo']:<{width}.2f} "
                    elif col == 'Fecha/Hora':
                        row_str += f"{fecha_str:<{width}} "
                    elif col == 'Usuario':
                        row_str += f"{reg['usuario']:<{width}} "
                    elif col == 'Motivo':
                        row_str += f"{motivo:<{width}} "
                
                print(row_str)
            
            print(separator)
            print(f"Total de registros mostrados: {len(registros)}\n")
            
            # Mostrar estadísticas básicas
            cursor.execute("""
                SELECT column_name, 
                       COUNT(*) as total_cambios,
                       MIN(fecha_cambio) as primer_cambio,
                       MAX(fecha_cambio) as ultimo_cambio,
                       AVG(peso_nuevo) as peso_promedio
                FROM peso_historial 
                GROUP BY column_name
            """)
            stats = cursor.fetchall()
            
            if stats:
                print("\n=== ESTADÍSTICAS POR COLUMNA ===")
                print("-" * 80)
                for stat in stats:
                    print(f"Columna: {stat['column_name']}")
                    print(f"• Total de cambios: {stat['total_cambios']}")
                    print(f"• Primer cambio: {stat['primer_cambio'].strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"• Último cambio: {stat['ultimo_cambio'].strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"• Peso promedio: {stat['peso_promedio']:.2f}")
                    print("-" * 40)
            
        except Exception as e:
            print(f"Error al mostrar historial: {e}")
        finally:
            if conn:
                conn.close()

    def actualizar_bd_desde_config(config_weights):
        # Actualizar usando manejo_peso en lugar de mysql directo
        from manejo_peso import actualizar_peso_columna
        for column, weight in config_weights.items():
            if not actualizar_peso_columna(column, float(weight), "system", "Sincronización desde config"):
                return False
        print("Pesos actualizados en la base de datos")
        return True

    def actualizar_config_desde_bd(db_weights):
        # Convertir Decimal a float antes de pasar al comando
        db_weights_float = {k: float(v) for k, v in db_weights.items()}
        from sincronizar_peso import update_weights
        if update_weights(db_weights_float):
            print("Pesos actualizados en el archivo de configuración")
            return True
        return False
    
    def solicitar_nuevos_pesos():
        """Solicita los nuevos pesos al usuario con validación"""
        nuevos_pesos = {}
        columnas = ['email', 'first_name', 'last_name']
        peso_min = 0.0
        peso_max = 100.0
        
        print(f"\nCONFIGURACIÓN DE NUEVOS PESOS")
        print(f"Rango permitido: {peso_min} - {peso_max}")
        
        for columna in columnas:
            while True:
                try:
                    peso_input = input(f"Ingrese el peso para {columna}: ")
                    peso = float(peso_input)
                    
                    if peso < peso_min:
                        print(f"El peso debe ser mayor o igual a {peso_min}")
                        continue
                    if peso > peso_max:
                        print(f"El peso debe ser menor o igual a {peso_max}")
                        continue
                        
                    nuevos_pesos[columna] = peso
                    print(f"{columna}: {peso}")
                    break
                except ValueError:
                    print("Por favor ingrese un número válido")
        
        return nuevos_pesos
    
    def confirmar_cambios(nuevos_pesos):
        """Permite al usuario confirmar los cambios después de la simulación"""
        print(f"\nRESUMEN DE CAMBIOS PROPUESTOS:")
        
        # Mostrar pesos actuales vs nuevos
        pesos_actuales, _ = obtener_pesos_columnas()
        for columna in nuevos_pesos.keys():
            peso_actual = pesos_actuales.get(columna, 0)
            peso_nuevo = nuevos_pesos[columna]
            diferencia = peso_nuevo - float(peso_actual)
            
            print(f"  • {columna}: {peso_actual} → {peso_nuevo} (diferencia: {diferencia:+.2f})")
        
        while True:
            confirmacion = input(f"\n¿Confirma la aplicación de estos cambios? (s/n): ").lower()
            if confirmacion in ['s', 'n']:
                return confirmacion == 's'
            print("Por favor responda 's' para sí o 'n' para no")

    while True:
        # Mostrar pesos actuales
        db_weights, db_timestamp, config_weights, config_timestamp = mostrar_pesos()
        
        # Mostrar menú principal
        print(f"\n=== MENÚ PRINCIPAL ===")
        print("1. Modificar pesos manualmente")
        print("2. Ver historial de cambios")
        print("3. Sincronizar automáticamente")
        print("4. Continuar con el matching")
        
        opcion = input("\nSeleccione una opción (1-4): ")
        
        if opcion == '1':
            print("\n¿Dónde desea realizar las modificaciones?")
            print("1. En la base de datos")
            print("2. En el archivo config")
            destino = input("Seleccione una opción (1/2): ")
            
            nuevos_pesos = solicitar_nuevos_pesos()
            
            # Solicitar motivo del cambio
            motivo = input("\nIngrese el motivo del cambio (opcional): ").strip()
            if not motivo:
                motivo = "Cambio manual de administrador"
            
            if destino == '1':
                # Aplicar cambios
                print(f"\nAPLICANDO CAMBIOS...")
                usuario = input("Ingrese su nombre de usuario (admin): ").strip() or "admin"
                
                exito_total = True
                for columna, peso in nuevos_pesos.items():
                    if not actualizar_peso_columna(columna, peso, usuario, motivo):
                        exito_total = False
                
                if exito_total:
                    print("Todos los cambios se aplicaron correctamente")
                    # Actualizar también el archivo config
                    respuesta = input("\n¿Desea actualizar también el archivo de configuración? (s/n): ").lower()
                    if respuesta == 's':
                        actualizar_config_desde_bd(nuevos_pesos)
                else:
                    print("Algunos cambios no se pudieron aplicar")
            else:
                from sincronizar_peso import update_weights
                if update_weights(nuevos_pesos):
                    print("Pesos actualizados en el archivo de configuración")
                    respuesta = input("\n¿Desea actualizar también la base de datos? (s/n): ").lower()
                    if respuesta == 's':
                        actualizar_bd_desde_config(nuevos_pesos)
                else:
                    print("Error al actualizar el archivo de configuración")
            
            # Mostrar pesos actualizados
            mostrar_pesos()
        
        elif opcion == '2':
            # Ver historial
            mostrar_historial()
        
        elif opcion == '3':
            # Sincronización automática
            if db_timestamp and config_timestamp:
                print("\nComparación de timestamps:")
                if config_timestamp > db_timestamp:
                    print("Los datos del archivo de configuración son más recientes")
                    print("Sincronizando automáticamente la base de datos...")
                    if actualizar_bd_desde_config(config_weights):
                        mostrar_pesos()
                elif db_timestamp > config_timestamp:
                    print("Los datos de la base de datos son más recientes")
                    print("Sincronizando automáticamente el archivo de configuración...")
                    if actualizar_config_desde_bd(db_weights):
                        mostrar_pesos()
                else:
                    print("Ambos orígenes tienen la misma fecha de actualización")
        
        elif opcion == '4':
            break
        
        else:
            print("Opción no válida. Por favor seleccione una opción del 1 al 4.")
    
    return True

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
if __name__ == "__main__":
    # Primero inicializar el sistema de pesos
    inicializar_sistema()
    
    # Luego ejecutar el matching
    pesos_personalizados = cargar_pesos_desde_config()[0]
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
