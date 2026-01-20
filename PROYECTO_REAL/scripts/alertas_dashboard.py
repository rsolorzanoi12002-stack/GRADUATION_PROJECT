# alertas_dashboard.py
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

class DashboardAlertas:
    def __init__(self):
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
    
    def mostrar_alertas_pendientes(self):
        """Muestra alertas pendientes de atencion"""
        print("\n" + "="*70)
        print("ALERTAS PENDIENTES - SISTEMA DE MONITOREO")
        print("="*70)
        
        if not os.path.exists(self.db_path):
            print("Base de datos no encontrada")
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            query = '''
                SELECT id, timestamp, nivel, tipo, ubicacion, mensaje
                FROM alertas_sistema
                WHERE procesada = 0
                ORDER BY 
                    CASE nivel
                        WHEN 'CRITICA' THEN 1
                        WHEN 'ADVERTENCIA' THEN 2
                        ELSE 3
                    END,
                    timestamp DESC
                LIMIT 20
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                print("No hay alertas pendientes ✓")
                return
            
            print(f"Total alertas pendientes: {len(df)}")
            
            # Separar por nivel
            criticas = df[df['nivel'] == 'CRITICA']
            advertencias = df[df['nivel'] == 'ADVERTENCIA']
            
            if not criticas.empty:
                print(f"\nALERTAS CRITICAS ({len(criticas)}):")
                for _, alerta in criticas.iterrows():
                    hora = alerta['timestamp'][11:19] if len(alerta['timestamp']) > 10 else alerta['timestamp']
                    print(f"   [{hora}] {alerta['ubicacion']}: {alerta['mensaje'][:60]}...")
            
            if not advertencias.empty:
                print(f"\nALERTAS DE ADVERTENCIA ({len(advertencias)}):")
                for _, alerta in advertencias.iterrows():
                    hora = alerta['timestamp'][11:19] if len(alerta['timestamp']) > 10 else alerta['timestamp']
                    print(f"   [{hora}] {alerta['ubicacion']}: {alerta['mensaje'][:60]}...")
            
            print(f"\nPara mas detalles, ejecute el sistema de alertas")
            
        except Exception as e:
            print(f"Error consultando alertas: {e}")
    
    def mostrar_estadisticas_alertas(self, horas=24):
        """Muestra estadisticas de alertas"""
        print("\n" + "="*70)
        print(f"ESTADISTICAS DE ALERTAS - Ultimas {horas} horas")
        print("="*70)
        
        if not os.path.exists(self.db_path):
            print("Base de datos no encontrada")
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Estadisticas generales
            query = f'''
                SELECT 
                    nivel,
                    COUNT(*) as total,
                    SUM(CASE WHEN procesada = 1 THEN 1 ELSE 0 END) as procesadas
                FROM alertas_sistema
                WHERE datetime(timestamp) >= datetime('now', '-{horas} hours')
                GROUP BY nivel
            '''
            df = pd.read_sql_query(query, conn)
            
            if not df.empty:
                print("\nResumen por nivel:")
                for _, row in df.iterrows():
                    pendientes = row['total'] - row['procesadas']
                    print(f"   {row['nivel']}: {row['total']} total, {row['procesadas']} procesadas, {pendientes} pendientes")
            
            # Alertas por tipo
            query = f'''
                SELECT tipo, COUNT(*) as cantidad
                FROM alertas_sistema
                WHERE datetime(timestamp) >= datetime('now', '-{horas} hours')
                GROUP BY tipo
                ORDER BY cantidad DESC
            '''
            df_tipos = pd.read_sql_query(query, conn)
            
            if not df_tipos.empty:
                print("\nAlertas por tipo:")
                for _, row in df_tipos.iterrows():
                    print(f"   {row['tipo']}: {row['cantidad']}")
            
            # Ubicaciones con mas alertas
            query = f'''
                SELECT ubicacion, COUNT(*) as cantidad
                FROM alertas_sistema
                WHERE datetime(timestamp) >= datetime('now', '-{horas} hours')
                GROUP BY ubicacion
                ORDER BY cantidad DESC
                LIMIT 5
            '''
            df_ubicaciones = pd.read_sql_query(query, conn)
            
            if not df_ubicaciones.empty:
                print("\nUbicaciones con mas alertas:")
                for _, row in df_ubicaciones.iterrows():
                    print(f"   {row['ubicacion']}: {row['cantidad']} alertas")
            
            conn.close()
            
        except Exception as e:
            print(f"Error obteniendo estadisticas: {e}")
    
    def menu_interactivo(self):
        """Menu interactivo para el dashboard de alertas"""
        while True:
            print("\n" + "="*70)
            print("DASHBOARD DE ALERTAS - SISTEMA DE MONITOREO")
            print("="*70)
            print("1. Ver alertas pendientes")
            print("2. Ver estadisticas (24h)")
            print("3. Ver estadisticas (12h)")
            print("4. Ver estadisticas (6h)")
            print("5. Volver al menu principal")
            print("-"*70)
            
            opcion = input("Seleccione una opcion (1-5): ").strip()
            
            if opcion == "1":
                self.mostrar_alertas_pendientes()
            elif opcion == "2":
                self.mostrar_estadisticas_alertas(24)
            elif opcion == "3":
                self.mostrar_estadisticas_alertas(12)
            elif opcion == "4":
                self.mostrar_estadisticas_alertas(6)
            elif opcion == "5":
                print("Saliendo del dashboard de alertas...")
                break
            else:
                print("Opcion invalida. Intente nuevamente.")
            
            if opcion in ["1", "2", "3", "4"]:
                input("\nPresione Enter para continuar...")

def main():
    """Funcion principal"""
    print("DASHBOARD DE ALERTAS - SISTEMA DE MONITOREO DE CALIDAD DEL AIRE")
    print("="*70)
    
    dashboard = DashboardAlertas()
    
    # Verificar si la base de datos existe
    if not os.path.exists(dashboard.db_path):
        print(f"Error: Base de datos no encontrada en {dashboard.db_path}")
        print("Ejecute primero procesador_json.py para procesar datos")
        return
    
    # Menu interactivo
    dashboard.menu_interactivo()

if __name__ == "__main__":
    main()