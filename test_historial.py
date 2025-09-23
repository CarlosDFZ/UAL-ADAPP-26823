#!/usr/bin/env python3
"""
Script de prueba del sistema de historial de pesos
"""

from peso_historial_manager import PesoHistorialManager, inicializar_historial
from manejo_peso import obtener_pesos_columnas
import time

def test_sistema_historial():
    print("🧪 INICIANDO PRUEBAS DEL SISTEMA DE HISTORIAL")
    print("=" * 60)
    
    # 1. Inicializar sistema
    print("\n1️⃣  Inicializando sistema de historial...")
    if not inicializar_historial():
        print("❌ Error en inicialización")
        return False
    
    # 2. Crear manager
    manager = PesoHistorialManager()
    
    # 3. Probar validaciones
    print("\n2️⃣  Probando validaciones...")
    
    test_cases = [
        (5.0, True, "Peso válido normal"),
        (-1.0, False, "Peso negativo"),
        (150.0, False, "Peso mayor al máximo"),
        (0.0, True, "Peso mínimo válido"),
        (100.0, True, "Peso máximo válido"),
        ("abc", False, "Valor no numérico")
    ]
    
    for peso, esperado, descripcion in test_cases:
        es_valido, mensaje = manager.validar_peso(peso)
        status = "✅" if es_valido == esperado else "❌"
        print(f"    {status} {descripcion}: {mensaje}")
    
    # 4. Probar actualización con historial
    print("\n3️⃣  Probando actualizaciones con historial...")
    
    # Obtener pesos actuales
    pesos_actuales, _ = obtener_pesos_columnas()
    if not pesos_actuales:
        print("❌ No se pudieron obtener pesos actuales")
        return False
    
    print(f"    Pesos actuales: {pesos_actuales}")
    
    # Hacer algunos cambios de prueba
    cambios_prueba = [
        ('email', 8.0, 'test_user', 'Prueba del sistema de historial'),
        ('first_name', 6.5, 'test_user', 'Ajuste de peso de nombre'),
        ('last_name', 7.5, 'test_user', 'Ajuste de peso de apellido')
    ]
    
    for columna, peso, usuario, motivo in cambios_prueba:
        print(f"    Actualizando {columna} a {peso}...")
        if manager.actualizar_peso_con_historial(columna, peso, usuario, motivo):
            print(f"    ✅ {columna} actualizada correctamente")
        else:
            print(f"    ❌ Error actualizando {columna}")
        
        # Pequeña pausa para que los timestamps sean diferentes
        time.sleep(1)
    
    # 5. Probar consulta de historial
    print("\n4️⃣  Probando consulta de historial...")
    manager.mostrar_historial(limit=5)
    
    # 6. Probar estadísticas
    print("\n5️⃣  Probando estadísticas...")
    manager.mostrar_estadisticas()
    
    # 7. Probar filtros
    print("\n6️⃣  Probando filtros...")
    print("    Historial filtrado por usuario 'test_user':")
    manager.mostrar_historial(usuario='test_user', limit=3)
    
    print("    Historial filtrado por columna 'email':")
    manager.mostrar_historial(column_name='email', limit=3)
    
    print("\n✅ TODAS LAS PRUEBAS COMPLETADAS")
    return True

def test_simulacion():
    print("\n🧪 PROBANDO SIMULACIÓN DE IMPACTO")
    print("=" * 60)
    
    manager = PesoHistorialManager()
    
    # Parámetros de prueba (mismos que en la aplicación principal)
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
    
    # Nuevos pesos para probar
    nuevos_pesos = {
        'email': 10.0,
        'first_name': 3.0,
        'last_name': 2.0
    }
    
    print(f"    Simulando con pesos: {nuevos_pesos}")
    
    try:
        exito = manager.simular_impacto_pesos(nuevos_pesos, params_dict, num_registros=5)
        if exito:
            print("✅ Simulación completada correctamente")
        else:
            print("❌ Error en la simulación")
    except Exception as e:
        print(f"❌ Error durante simulación: {e}")

if __name__ == "__main__":
    print("🚀 INICIANDO SUITE DE PRUEBAS")
    
    try:
        # Ejecutar pruebas principales
        if test_sistema_historial():
            print("\n" + "=" * 60)
            # Ejecutar prueba de simulación
            test_simulacion()
        
        print(f"\n🎉 PRUEBAS FINALIZADAS")
        
    except KeyboardInterrupt:
        print(f"\n❌ Pruebas canceladas por el usuario")
    except Exception as e:
        print(f"\n❌ Error durante las pruebas: {e}")
        import traceback
        traceback.print_exc()