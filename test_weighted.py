from fuzz_functions import execute_dynamic_matching, execute_dynamic_matching_weighted
import pandas as pd

# Configuración de prueba
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

# Pesos personalizados
from config import MATCHING_WEIGHTS
pesos_personalizados = MATCHING_WEIGHTS
total_pesos = sum(pesos_personalizados.values())

print("INICIANDO PRUEBA DEL SISTEMA DE PONDERACION")
print("=" * 70)
print(f"Pesos configurados: {pesos_personalizados}")
print(f"Suma total de pesos: {total_pesos}")
print("=" * 70)

# Ejecutar ambos sistemas
print("Ejecutando sistema tradicional (sin ponderacion)...")
resultados_tradicionales = execute_dynamic_matching(params_dict, score_cutoff=70)

print("Ejecutando sistema ponderado...")
resultados_ponderados = execute_dynamic_matching_weighted(params_dict, score_cutoff=70, column_weights=pesos_personalizados)

# Crear DataFrames para comparación
df_tradicional = pd.DataFrame(resultados_tradicionales)
df_ponderado = pd.DataFrame(resultados_ponderados)

# Mostrar estadísticas generales
print("\nESTADISTICAS GENERALES")
print("=" * 70)
print(f"Registros procesados: {len(df_tradicional)}")
print(f"Puntuacion promedio tradicional: {df_tradicional['score'].mean():.2f}%")
print(f"Puntuacion promedio ponderada: {df_ponderado['score'].mean():.2f}%")

# Analizar diferencias significativas
df_comparativo = pd.DataFrame({
    'source_first_name': df_tradicional['first_name'],
    'source_last_name': df_tradicional['last_name'],
    'source_email': df_tradicional['email'],
    'score_tradicional': df_tradicional['score'],
    'score_ponderado': df_ponderado['score'],
    'diferencia': df_ponderado['score'] - df_tradicional['score']
})

# Identificar casos donde hay diferencias significativas (> 5 puntos)
df_diferencias = df_comparativo[abs(df_comparativo['diferencia']) > 5]

print(f"\nREGISTROS CON DIFERENCIAS SIGNIFICATIVAS (> 5 puntos): {len(df_diferencias)}")
print("=" * 70)

if len(df_diferencias) > 0:
    for _, row in df_diferencias.iterrows():
        print(f"\nREGISTRO ORIGINAL:")
        print(f"Nombre: {row['source_first_name']} {row['source_last_name']}")
        print(f"Email: {row['source_email']}")
        print(f"Puntuacion tradicional: {row['score_tradicional']}%")
        print(f"Puntuacion ponderada: {row['score_ponderado']}%")
        print(f"Diferencia: {row['diferencia']:+.2f} puntos")
        
        # Buscar el registro completo para mostrar comparaciones detalladas
        registro_completo = next((r for r in resultados_ponderados 
                                if r['first_name'] == row['source_first_name'] 
                                and r['last_name'] == row['source_last_name'] 
                                and r['email'] == row['source_email']), None)
        
        if registro_completo and 'column_scores' in registro_completo and 'match_result_values' in registro_completo:
            print(f"\nCOMPARADO CON:")
            print(f"Nombre match: {registro_completo['match_result_values'].get('first_name', 'N/A')}")
            print(f"Apellido match: {registro_completo['match_result_values'].get('last_name', 'N/A')}")
            print(f"Email match: {registro_completo['match_result_values'].get('email', 'N/A')}")
            
            print("\nCALCULO DETALLADO:")
            print("-" * 50)
            total_ponderado = 0
            
            for columna, peso in pesos_personalizados.items():
                if columna in registro_completo['column_scores']:
                    valor_origen = registro_completo.get(columna, '')
                    valor_match = registro_completo['match_result_values'].get(columna, '')
                    puntuacion = registro_completo['column_scores'][columna]
                    contribucion = (puntuacion * peso) / total_pesos
                    total_ponderado += contribucion
                    
                    print(f"{columna.upper()}:")
                    print(f"  Origen: '{valor_origen}'")
                    print(f"  Match:  '{valor_match}'")
                    print(f"  Similitud: {puntuacion}%")
                    print(f"  Peso: {peso}")
                    print(f"  Contribucion: {puntuacion}% x {peso} = {contribucion:.2f}%")
                    print()
            
            print(f"SUMA PONDERADA: {total_ponderado:.2f}%")
            print(f"PUNTUACION FINAL: {registro_completo['score']}%")
        
        print("=" * 50)
else:
    print("No se encontraron diferencias significativas entre los sistemas")

# Mostrar ejemplos representativos con comparaciones detalladas
print("\nEJEMPLOS DETALLADOS CON COMPARACIONES")
print("=" * 70)

# Mostrar primeros 2-3 registros con comparaciones completas
for i, registro in enumerate(resultados_ponderados[:3]):
    if 'column_scores' in registro and 'match_result_values' in registro:
        print(f"\nEJEMPLO {i + 1}:")
        print("=" * 50)
        print(f"REGISTRO ORIGINAL:")
        print(f"  Nombre: {registro.get('first_name', '')}")
        print(f"  Apellido: {registro.get('last_name', '')}")
        print(f"  Email: {registro.get('email', '')}")
        
        print(f"\nCOMPARADO CON:")
        print(f"  Nombre: {registro['match_result_values'].get('first_name', 'N/A')}")
        print(f"  Apellido: {registro['match_result_values'].get('last_name', 'N/A')}")
        print(f"  Email: {registro['match_result_values'].get('email', 'N/A')}")
        
        # Encontrar puntuacion tradicional para comparar
        puntuacion_tradicional = next((r['score'] for r in resultados_tradicionales 
                                     if r['first_name'] == registro.get('first_name') 
                                     and r['last_name'] == registro.get('last_name') 
                                     and r['email'] == registro.get('email')), 'N/A')
        
        print(f"\nRESULTADOS:")
        print(f"  Tradicional: {puntuacion_tradicional}%")
        print(f"  Ponderado: {registro['score']}%")
        
        print("\nDESGLOSE POR COLUMNA:")
        print("-" * 40)
        total_contribucion = 0
        
        for columna, peso in pesos_personalizados.items():
            if columna in registro['column_scores']:
                valor_origen = registro.get(columna, '')
                valor_match = registro['match_result_values'].get(columna, '')
                puntuacion_col = registro['column_scores'][columna]
                contribucion = (puntuacion_col * peso) / total_pesos
                total_contribucion += contribucion
                
                print(f"{columna.upper()}:")
                print(f"  Origen: '{valor_origen}'")
                print(f"  Match:  '{valor_match}'") 
                print(f"  Similitud: {puntuacion_col}%")
                print(f"  Peso: {peso}")
                print(f"  Contribucion: {puntuacion_col}% x {peso} = {contribucion:.2f}%")
                print()
        
        print(f"SUMA PONDERADA: {total_contribucion:.2f}%")
        print(f"PUNTUACION FINAL: {registro['score']}%")
        print("=" * 50)

# Categorización y cambios
umbral_alto = 97
umbral_medio = 85

def categorizar_puntuacion(score):
    if score >= umbral_alto:
        return "Alta"
    elif score >= umbral_medio:
        return "Media"
    else:
        return "Baja"

df_comparativo['categoria_tradicional'] = df_comparativo['score_tradicional'].apply(categorizar_puntuacion)
df_comparativo['categoria_ponderado'] = df_comparativo['score_ponderado'].apply(categorizar_puntuacion)

cambios_categoria = df_comparativo[df_comparativo['categoria_tradicional'] != df_comparativo['categoria_ponderado']]

print(f"\nCAMBIOS EN CATEGORIZACION: {len(cambios_categoria)} registros")
print("=" * 70)

for _, row in cambios_categoria.iterrows():
    print(f"\nCAMBIO DE CATEGORIA:")
    print(f"Nombre: {row['source_first_name']} {row['source_last_name']}")
    print(f"Email: {row['source_email']}")
    print(f"Tradicional: {row['score_tradicional']}% ({row['categoria_tradicional']})")
    print(f"Ponderado: {row['score_ponderado']}% ({row['categoria_ponderado']})")
    
    # Mostrar comparación que causó el cambio
    registro = next((r for r in resultados_ponderados 
                   if r['first_name'] == row['source_first_name'] 
                   and r['last_name'] == row['source_last_name'] 
                   and r['email'] == row['source_email']), None)
    
    if registro and 'column_scores' in registro and 'match_result_values' in registro:
        print(f"\nCOMPARACION QUE CAUSO EL CAMBIO:")
        print(f"Original: {registro.get('first_name', '')} {registro.get('last_name', '')} - {registro.get('email', '')}")
        print(f"Match:    {registro['match_result_values'].get('first_name', '')} {registro['match_result_values'].get('last_name', '')} - {registro['match_result_values'].get('email', '')}")
        
        print("\nPUNTUACIONES INDIVIDUALES:")
        for columna in pesos_personalizados.keys():
            if columna in registro['column_scores']:
                print(f"  {columna}: {registro['column_scores'][columna]}%")
    
    print("-" * 50)

# Resumen final
print("\n" + "=" * 70)
print("RESUMEN FINAL")
print("=" * 70)
print(f"Registros procesados: {len(df_comparativo)}")
print(f"Diferencias significativas: {len(df_diferencias)}")
print(f"Cambios de categoria: {len(cambios_categoria)}")
print(f"\nPesos aplicados: {pesos_personalizados}")
print(f"Formula: (nombrex{MATCHING_WEIGHTS['first_name']} + apellido×{MATCHING_WEIGHTS['last_name']} + email×{MATCHING_WEIGHTS['email']}) ÷ {total_pesos}")