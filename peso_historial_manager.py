from datetime import datetime, date
import pandas as pd
from fuzz_functions import conectar_bd, execute_dynamic_matching_weighted
from decimal import Decimal
import socket

class PesoHistorialManager:
    def __init__(self):
        self.peso_min = 0.0
        self.peso_max = 100.0
        
    def _obtener_ip_local(self):
        """Obtiene la IP local del usuario"""
        try:
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            return ip_address
        except:
            return "127.0.0.1"
    
    def validar_peso(self, peso):
        """Valida que el peso esté dentro del rango permitido"""
        try:
            peso_float = float(peso)
            if peso_float < self.peso_min:
                return False, f"El peso {peso_float} es menor al mínimo permitido ({self.peso_min})"
            if peso_float > self.peso_max:
                return False, f"El peso {peso_float} es mayor al máximo permitido ({self.peso_max})"
            return True, "Peso válido"
        except (ValueError, TypeError):
            return False, "El peso debe ser un número válido"
    
    def obtener_peso_actual(self, column_name):
        """Obtiene el peso actual de una columna"""
        try:
            conn = conectar_bd('dbo')
            if not conn:
                return None
            
            cursor = conn.cursor()
            cursor.execute("SELECT weight FROM peso_columnas WHERE column_name = %s", (column_name,))
            result = cursor.fetchone()
            return float(result[0]) if result else None
        except Exception as e:
            print(f"Error al obtener peso actual: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def registrar_cambio_historial(self, column_name, peso_anterior, peso_nuevo, usuario="admin", motivo=None):
        """Registra un cambio de peso en el historial"""
        try:
            conn = conectar_bd('dbo')
            if not conn:
                return False
                
            cursor = conn.cursor()
            ip_address = self._obtener_ip_local()
            
            cursor.callproc('InsertPesoHistorial', (
                column_name,
                float(peso_anterior),
                float(peso_nuevo),
                usuario,
                motivo
            ))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error al registrar en historial: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def actualizar_peso_con_historial(self, column_name, peso_nuevo, usuario="admin", motivo=None):
        """Actualiza el peso de una columna registrando el cambio en el historial"""
        # Validar el nuevo peso
        es_valido, mensaje = self.validar_peso(peso_nuevo)
        if not es_valido:
            print(f"Error de validación: {mensaje}")
            return False
        
        # Obtener peso actual
        peso_anterior = self.obtener_peso_actual(column_name)
        if peso_anterior is None:
            print(f"No se pudo obtener el peso actual de la columna '{column_name}'")
            return False
        
        # Verificar si el peso realmente cambió
        if abs(float(peso_anterior) - float(peso_nuevo)) < 0.001:
            print(f"El peso de '{column_name}' ya es {peso_nuevo}. No se realizaron cambios.")
            return True
        
        try:
            conn = conectar_bd('dbo')
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            # Actualizar peso en la tabla principal
            cursor.execute("""
                UPDATE peso_columnas 
                SET weight = %s, last_updated = CURRENT_TIMESTAMP 
                WHERE column_name = %s
            """, (float(peso_nuevo), column_name))
            
            # Registrar en historial
            self.registrar_cambio_historial(column_name, peso_anterior, peso_nuevo, usuario, motivo)
            
            conn.commit()
            print(f"Peso de '{column_name}' actualizado: {peso_anterior} → {peso_nuevo}")
            return True
            
        except Exception as e:
            print(f"Error al actualizar peso: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def obtener_historial(self, column_name=None, fecha_inicio=None, fecha_fin=None, 
                         usuario=None, limit=None):
        """Obtiene el historial de cambios con filtros opcionales"""
        try:
            conn = conectar_bd('dbo')
            if not conn:
                return None
                
            cursor = conn.cursor(dictionary=True)
            cursor.callproc('GetPesoHistorial', (
                column_name,
                fecha_inicio,
                fecha_fin,
                usuario,
                limit
            ))
            
            # Obtener resultados
            for result in cursor.stored_results():
                return result.fetchall()
            
            return []
        except Exception as e:
            print(f"Error al obtener historial: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def mostrar_historial(self, column_name=None, fecha_inicio=None, fecha_fin=None, 
                         usuario=None, limit=20):
        """Muestra el historial de cambios de forma formateada"""
        historial = self.obtener_historial(column_name, fecha_inicio, fecha_fin, usuario, limit)
        
        if not historial:
            print("No se encontraron registros en el historial.")
            return
        
        print(f"\n=== HISTORIAL DE CAMBIOS DE PESOS ===")
        if column_name:
            print(f"Filtrado por columna: {column_name}")
        if fecha_inicio or fecha_fin:
            print(f"Rango de fechas: {fecha_inicio or 'inicio'} - {fecha_fin or 'fin'}")
        if usuario:
            print(f"Usuario: {usuario}")
        
        print("\n" + "="*120)
        print(f"{'ID':<4} {'Columna':<15} {'Anterior':<10} {'Nuevo':<10} {'Fecha/Hora':<20} {'Usuario':<15} {'Motivo':<25}")
        print("="*120)
        
        for record in historial:
            fecha_str = record['fecha_cambio'].strftime('%Y-%m-%d %H:%M:%S')
            motivo = record.get('motivo', '')[:24] + '...' if len(record.get('motivo', '')) > 24 else record.get('motivo', '')
            
            print(f"{record['id']:<4} {record['column_name']:<15} {record['peso_anterior']:<10} "
                  f"{record['peso_nuevo']:<10} {fecha_str:<20} {record.get('usuario', 'N/A'):<15} {motivo:<25}")
        
        print("="*120)
        print(f"Total de registros: {len(historial)}")
    
    def obtener_estadisticas(self):
        """Obtiene estadísticas del historial de cambios"""
        try:
            conn = conectar_bd('dbo')
            if not conn:
                return None
                
            cursor = conn.cursor(dictionary=True)
            cursor.callproc('GetPesoEstadisticas')
            
            for result in cursor.stored_results():
                return result.fetchall()
            
            return []
        except Exception as e:
            print(f"Error al obtener estadísticas: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def mostrar_estadisticas(self):
        """Muestra estadísticas del historial"""
        stats = self.obtener_estadisticas()
        
        if not stats:
            print("No hay estadísticas disponibles.")
            return
        
        print("\n=== ESTADÍSTICAS DEL HISTORIAL ===")
        print("="*90)
        print(f"{'Columna':<15} {'Cambios':<8} {'Primer Cambio':<20} {'Último Cambio':<20} {'Peso Prom.':<12}")
        print("="*90)
        
        for stat in stats:
            primer_cambio = stat['primer_cambio'].strftime('%Y-%m-%d %H:%M:%S') if stat['primer_cambio'] else 'N/A'
            ultimo_cambio = stat['ultimo_cambio'].strftime('%Y-%m-%d %H:%M:%S') if stat['ultimo_cambio'] else 'N/A'
            peso_prom = f"{float(stat['peso_promedio']):.2f}" if stat['peso_promedio'] else 'N/A'
            
            print(f"{stat['column_name']:<15} {stat['total_cambios']:<8} {primer_cambio:<20} "
                  f"{ultimo_cambio:<20} {peso_prom:<12}")
        
        print("="*90)
    
    def simular_impacto_pesos(self, nuevos_pesos, datos_prueba_params, num_registros=10):
        """Simula el impacto de nuevos pesos en datos de prueba"""
        print(f"\n=== SIMULACIÓN DE IMPACTO DE NUEVOS PESOS ===")
        print("Comparando resultados con pesos actuales vs nuevos pesos...")
        
        try:
            # Obtener pesos actuales
            from manejo_peso import obtener_pesos_columnas
            pesos_actuales, _ = obtener_pesos_columnas()
            
            if not pesos_actuales:
                print("No se pudieron obtener los pesos actuales")
                return False
            
            print(f"\nPESOS ACTUALES:")
            for col, peso in pesos_actuales.items():
                print(f"  - {col}: {peso}")
            
            print(f"\nNUEVOS PESOS (PROPUESTOS):")
            for col, peso in nuevos_pesos.items():
                print(f"  - {col}: {peso}")
            
            # Ejecutar matching con pesos actuales
            print(f"\nEjecutando matching con pesos actuales...")
            resultados_actuales = execute_dynamic_matching_weighted(
                datos_prueba_params, 
                score_cutoff=70, 
                column_weights=pesos_actuales
            )
            
            # Ejecutar matching con nuevos pesos
            print(f"Ejecutando matching con nuevos pesos...")
            resultados_nuevos = execute_dynamic_matching_weighted(
                datos_prueba_params, 
                score_cutoff=70, 
                column_weights=nuevos_pesos
            )
            
            # Comparar resultados
            self._comparar_resultados_simulacion(
                resultados_actuales[:num_registros], 
                resultados_nuevos[:num_registros],
                pesos_actuales,
                nuevos_pesos
            )
            
            return True
            
        except Exception as e:
            print(f"Error en simulación: {e}")
            return False
    
    def _comparar_resultados_simulacion(self, resultados_actuales, resultados_nuevos, 
                                       pesos_actuales, nuevos_pesos):
        """Compara y muestra los resultados de la simulación"""
        
        print(f"\n=== COMPARACIÓN DE RESULTADOS (PRIMEROS {len(resultados_actuales)} REGISTROS) ===")
        print("="*100)
        print(f"{'Registro':<8} {'Score Actual':<12} {'Score Nuevo':<12} {'Diferencia':<12} {'Impacto':<15}")
        print("="*100)
        
        mejoras = 0
        empeoramientos = 0
        sin_cambio = 0
        
        for i, (actual, nuevo) in enumerate(zip(resultados_actuales, resultados_nuevos)):
            score_actual = actual.get('score', 0)
            score_nuevo = nuevo.get('score', 0)
            diferencia = score_nuevo - score_actual
            
            if abs(diferencia) < 0.01:
                impacto = "Sin cambio"
                sin_cambio += 1
            elif diferencia > 0:
                impacto = "Mejora"
                mejoras += 1
            else:
                impacto = "Empeora"
                empeoramientos += 1
            
            print(f"{i+1:<8} {score_actual:<12.2f} {score_nuevo:<12.2f} {diferencia:<12.2f} {impacto:<15}")
        
        print("="*100)
        
        # Resumen de impacto
        total = len(resultados_actuales)
        print(f"\nRESUMEN DEL IMPACTO:")
        print(f"  • Mejoras: {mejoras}/{total} ({mejoras/total*100:.1f}%)")
        print(f"  • Empeoramientos: {empeoramientos}/{total} ({empeoramientos/total*100:.1f}%)")
        print(f"  • Sin cambio: {sin_cambio}/{total} ({sin_cambio/total*100:.1f}%)")
        
        # Calcular promedio de scores
        promedio_actual = sum(r.get('score', 0) for r in resultados_actuales) / len(resultados_actuales)
        promedio_nuevo = sum(r.get('score', 0) for r in resultados_nuevos) / len(resultados_nuevos)
        
        print(f"\nPROMEDIOS:")
        print(f"  • Score promedio actual: {promedio_actual:.2f}")
        print(f"  • Score promedio nuevo: {promedio_nuevo:.2f}")
        print(f"  • Diferencia promedio: {promedio_nuevo - promedio_actual:.2f}")
        
        if promedio_nuevo > promedio_actual:
            print("Los nuevos pesos mejoran el rendimiento general")
        elif promedio_nuevo < promedio_actual:
            print("Los nuevos pesos reducen el rendimiento general")
        else:
            print("Los nuevos pesos mantienen el rendimiento general")

def inicializar_historial():
    """Inicializa las tablas y procedimientos del historial"""
    try:
        conn = conectar_bd('dbo')
        if not conn:
            print("No se pudo conectar a la base de datos")
            return False
            
        cursor = conn.cursor()
        
        # Leer y ejecutar el script SQL
        with open('peso_historial.sql', 'r', encoding='utf-8') as file:
            sql_commands = file.read()
        
        # Dividir por DELIMITER y ejecutar
        commands = sql_commands.split('DELIMITER')
        
        for i, command_block in enumerate(commands):
            if i == 0:  # Primer bloque (sin delimiter)
                sub_commands = command_block.strip().split(';')
                for cmd in sub_commands:
                    if cmd.strip():
                        cursor.execute(cmd)
            else:
                # Bloques con delimiter
                lines = command_block.strip().split('\n')
                if len(lines) > 1:
                    # Remover la línea del delimiter
                    sql_block = '\n'.join(lines[1:])
                    if sql_block.strip():
                        cursor.execute(sql_block)
        
        conn.commit()
        print("Sistema de historial inicializado correctamente")
        return True
        
    except Exception as e:
        print(f"Error al inicializar historial: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Prueba del sistema
    manager = PesoHistorialManager()
    inicializar_historial()
    print("Sistema de historial de pesos listo para usar")