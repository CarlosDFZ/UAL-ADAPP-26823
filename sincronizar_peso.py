from datetime import datetime
from manejo_peso import obtener_pesos_columnas, actualizar_peso_columna

def cargar_pesos_desde_config():
    try:
        import config
        #recorre el diccionario y convierte los valores a float
        weights = {k: float(v) for k, v in config.MATCHING_WEIGHTS.items()}
        timestamp = datetime.strptime(config.LAST_UPDATED, "%Y-%m-%d %H:%M:%S")
        return weights, timestamp
    except Exception as e:
        print(f"Error al cargar los pesos desde config: {e}")
        return None, None
    
def guardar_pesos_en_config(weights, timestamp):
    try:
        with open('config.py', 'r', encoding='utf-8') as file:
            lines = file.readlines()
        weights_str = "MATCHING_WEIGHTS = {\n"
        for column, weight in weights.items():
            #Concatena la columna variable y el peso, .2f para dos decimales 
            weights_str += f"    '{column}': {weight:.2f},\n"
        weights_str += "}\n"
        timestamp_str = f'LAST_UPDATED = "{timestamp.strftime("%Y-%m-%d %H:%M:%S")}"\n'
        with open('config.py', 'w', encoding='utf-8') as file:
            in_weights = False
            in_timestamp = False
            for line in lines:
                if line.startswith("MATCHING_WEIGHTS"):
                    file.write(weights_str)
                    in_weights = True
                elif line.startswith("LAST_UPDATED"):
                    file.write(timestamp_str)
                    in_timestamp = True
                elif in_weights and line.startswith("}"):
                    in_weights = False
                    continue
                elif not in_weights and not in_timestamp:
                    file.write(line)
        return True
    except Exception as e:
        print(f"Error al guardar los pesos en config: {e}")
        return False
    
def sincronizar_pesos():
    db_weights, db_timestamp = obtener_pesos_columnas()
    config_weights, config_timestamp = cargar_pesos_desde_config()
    if not db_weights and not config_weights:
        print("No hay pesos en la base de datos ni en config. No se realiza ninguna acción.")
        return False
    if config_timestamp > db_timestamp:
        print("Los pesos en config son más recientes. Actualizando la base de datos...")
        for column, weight in config_weights.items():
            if not actualizar_peso_columna(column, weight):
                return False
    else:
        print("Los pesos en la base de datos son más recientes. Actualizando config.py...")
        if not guardar_pesos_en_config(db_weights, db_timestamp):
            return False
    print("Sincronización de pesos completada.")
    return True

if __name__ == "__main__":
    sincronizar_pesos()
        