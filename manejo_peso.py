from fuzz_functions import conectar_bd
from peso_historial_manager import PesoHistorialManager

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

def actualizar_peso_columna(column_name, weight, updated_by="system", motivo=None):
    """Actualiza el peso de una columna y registra el cambio en el historial"""
    try:
        conn = conectar_bd('dbo')
        if not conn:
            raise Exception("No se pudo conectar a la base de datos.")
        cursor = conn.cursor()
        
        # Obtener el peso actual antes de actualizar
        cursor.execute("SELECT weight FROM peso_columnas WHERE column_name = %s", (column_name,))
        result = cursor.fetchone()
        peso_anterior = float(result[0]) if result else 0.0
        
        # Actualizar peso en la tabla principal
        sql = """
        INSERT INTO peso_columnas (column_name, weight, updated_by)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            weight = VALUES(weight),
            updated_by = VALUES(updated_by)
        """
        cursor.execute(sql, (column_name, weight, updated_by))
        
        # Registrar en el historial
        sql_historial = """
        INSERT INTO peso_historial (column_name, peso_anterior, peso_nuevo, usuario, motivo)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql_historial, (column_name, peso_anterior, weight, updated_by, motivo))
        
        conn.commit()
        print(f"Peso de la columna '{column_name}' actualizado: {peso_anterior} → {weight}")
        return True
    except Exception as e:
        print(f"Error al actualizar el peso de la columna: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            
if __name__ == "__main__":
    from peso_historial_manager import inicializar_historial
    
    # Inicializar sistema de historial
    print("Inicializando sistema de historial...")
    inicializar_historial()
    
    # Ejecutar procedimientos de pesos
    peso_columnas_procedures()
    
    print("Sistema completo inicializado correctamente")