from fuzz_functions import conectar_bd

#Guarda la informacion de las columnas y sus pesos en la base de datos

def peso_columnas_procedures():
    try:
        conn = conectar_bd('dbo')
        if not conn:
            raise Exception("No se pudo conectar a la base de datos.")
        cursor = conn.cursor()
        
        with open('peso_columnas.sql', 'r', encoding='utf-8') as file:
            sql_commands = file.read()
        #Recorrer y ejecutar cada comando SQL
        for command in sql_commands.split(';'):
            if command.strip():
                cursor.execute(command)
        conn.commit()
        print("Procedimientos almacenados ejecutados correctamente.")
    except Exception as e:
        print(f"Error al ejecutar los procedimientos almacenados: {e}")
    finally:
        if conn:
            conn.close()
            
def obtener_pesos_columnas():
    try:
        conn = conectar_bd('dbo')
        if not conn:
            raise Exception("No se pudo conectar a la base de datos.")
        cursor = conn.cursor(dictionary=True)
        cursor.callproc('GetColumnWeights')
        # Obtener los resultados
        for result in cursor.stored_results():
            rows = result.fetchall()
            weights = {row['column_name']: row['weight'] for row in rows}
            last_updated = max(row['last_updated'] for row in rows)
            return weights, last_updated
    except Exception as e:
        print(f"Error al obtener los pesos de las columnas: {e}")
        return None, None
    finally:
        if conn:
            conn.close()
        
def actualizar_peso_columna(column_name, weight, updated_by="system"):
    try:
        conn = conectar_bd('dbo')
        if not conn:
            raise Exception("No se pudo conectar a la base de datos.")
        cursor = conn.cursor()
        cursor.callproc('UpdateColumnWeight', (column_name, weight, updated_by))
        conn.commit()
        print(f"Peso de la columna '{column_name}' actualizado a {weight}.")
        return True
    except Exception as e:
        print(f"Error al actualizar el peso de la columna: {e}")
        return False
    finally:
        if conn:
            conn.close()
            
if __name__ == "__main__":
    peso_columnas_procedures()