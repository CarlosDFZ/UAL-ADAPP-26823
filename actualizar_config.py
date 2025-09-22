from datetime import datetime
import re

def update_weights(weights):
    try:
        print(f"Intentando actualizar con pesos: {weights}")
        # Leer el archivo actual
        with open('config.py', 'r', encoding='utf-8') as file:
            content = file.read()
        
        print(f"Contenido leído: {content}")
        # Actualizar los pesos
        weights_str = "MATCHING_WEIGHTS = {\n"
        for key, value in weights.items():
            weights_str += f"    '{key}': {value:.2f},    # Peso para {key}\n"
        weights_str += "}"
        
        # Reemplazar la sección de pesos
        content = re.sub(
            r'MATCHING_WEIGHTS = \{[^}]+\}',
            weights_str,
            content
        )
        
        # Actualizar el timestamp - Modificado para manejar comillas dobles
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        content = re.sub(
            r'LAST_UPDATED = "[^"]+"',  # Cambiado para buscar comillas dobles
            f'LAST_UPDATED = "{current_time}"',
            content
        )
        
        # Guardar los cambios
        with open('config.py', 'w', encoding='utf-8') as file:
            file.write(content)
            
        print(f"Pesos actualizados con timestamp: {current_time}")
        return True
        
    except Exception as e:
        print(f"Error detallado: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        return False

if __name__ == "__main__":
    print("Para actualizar los pesos, usa esta función:")
    print("from actualizar_config import update_weights")
    print("update_weights({'email': 12.00, 'first_name': 6.00, 'last_name': 9.00})")
    

# mysql -u root -p dbo -e "CALL GetColumnWeights();"

# mysql -u root -p dbo -e "CALL UpdateColumnWeights('email', 12.00, 'system');"

# mysql -u root -p dbo -e "CALL UpdateColumnWeights('first_name', 6.00, 'system');"

# mysql -u root -p dbo -e "CALL UpdateColumnWeights('last_name', 9.00, 'system');"
    
# python -c "from actualizar_config import update_weights; update_weights({'email': 12.00, 'first_name': 6.00, 'last_name': 9.00})"