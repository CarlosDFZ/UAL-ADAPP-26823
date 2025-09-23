#!/usr/bin/env python3
"""
Script de línea de comandos para consultar el historial de cambios de pesos
Uso: python consultar_historial.py [opciones]
"""

import argparse
from datetime import datetime, date
from peso_historial_manager import PesoHistorialManager

def parse_date(date_string):
    """Convierte string de fecha a objeto date"""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Fecha inválida: {date_string}. Use formato YYYY-MM-DD")

def main():
    parser = argparse.ArgumentParser(
        description="Consulta el historial de cambios de pesos de columnas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python consultar_historial.py                           # Mostrar últimos 20 registros
  python consultar_historial.py -c email                  # Filtrar por columna 'email'
  python consultar_historial.py -u admin                  # Filtrar por usuario 'admin'
  python consultar_historial.py -l 50                     # Mostrar últimos 50 registros
  python consultar_historial.py --inicio 2025-01-01      # Desde fecha específica
  python consultar_historial.py --fin 2025-12-31         # Hasta fecha específica
  python consultar_historial.py -c email -u admin -l 10  # Combinando filtros
  python consultar_historial.py --stats                   # Mostrar estadísticas
        """
    )
    
    parser.add_argument('-c', '--columna', 
                       help='Filtrar por nombre de columna específica')
    
    parser.add_argument('-u', '--usuario', 
                       help='Filtrar por usuario que realizó el cambio')
    
    parser.add_argument('--inicio', type=parse_date,
                       help='Fecha de inicio (formato YYYY-MM-DD)')
    
    parser.add_argument('--fin', type=parse_date,
                       help='Fecha de fin (formato YYYY-MM-DD)')
    
    parser.add_argument('-l', '--limite', type=int, default=20,
                       help='Número máximo de registros a mostrar (default: 20)')
    
    parser.add_argument('--stats', action='store_true',
                       help='Mostrar estadísticas del historial en lugar del listado')
    
    parser.add_argument('--export', choices=['csv', 'excel'],
                       help='Exportar resultados a archivo CSV o Excel')
    
    args = parser.parse_args()
    
    # Crear manager
    manager = PesoHistorialManager()
    
    if args.stats:
        # Mostrar estadísticas
        manager.mostrar_estadisticas()
    else:
        # Mostrar historial con filtros
        print(f"🔍 Consultando historial de cambios de pesos...")
        
        if args.columna or args.usuario or args.inicio or args.fin:
            print(f"Filtros aplicados:")
            if args.columna:
                print(f"  - Columna: {args.columna}")
            if args.usuario:
                print(f"  - Usuario: {args.usuario}")
            if args.inicio:
                print(f"  - Desde: {args.inicio}")
            if args.fin:
                print(f"  - Hasta: {args.fin}")
            if args.limite != 20:
                print(f"  - Límite: {args.limite}")
        
        manager.mostrar_historial(
            column_name=args.columna,
            fecha_inicio=args.inicio,
            fecha_fin=args.fin,
            usuario=args.usuario,
            limit=args.limite
        )
        
        # Exportar si se solicitó
        if args.export:
            historial = manager.obtener_historial(
                column_name=args.columna,
                fecha_inicio=args.inicio,
                fecha_fin=args.fin,
                usuario=args.usuario,
                limit=args.limite
            )
            
            if historial:
                export_historial(historial, args.export)

def export_historial(historial, formato):
    """Exporta el historial a archivo CSV o Excel"""
    import pandas as pd
    from datetime import datetime
    
    # Convertir a DataFrame
    df = pd.DataFrame(historial)
    
    # Formatear fecha
    if 'fecha_cambio' in df.columns:
        df['fecha_cambio'] = pd.to_datetime(df['fecha_cambio'])
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if formato == 'csv':
        filename = f'historial_pesos_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Historial exportado a: {filename}")
    
    elif formato == 'excel':
        filename = f'historial_pesos_{timestamp}.xlsx'
        df.to_excel(filename, index=False)
        print(f"Historial exportado a: {filename}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperación cancelada por el usuario")
    except Exception as e:
        print(f"Error: {e}")