import os
import sqlite3
import json
import pandas as pd

class ConsultaBaseDatos:
    def __init__(self):
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
    def conectar(self):
        """Conecta a la base de datos"""
        if not os.path.exists(self.db_path):
            print(f"Error: Base de datos no encontrada en {self.db_path}")
            return None
        return sqlite3.connect(self.db_path)
    
    def mostrar_tablas(self):
        """Muestra todas las tablas en la base de datos"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tablas = cursor.fetchall()
            print("TABLAS EN LA BASE DE DATOS:")
            print("-" * 40)
            for tabla in tablas:
                print(f"• {tabla[0]}")
            print()
    
    def contar_registros(self):
        """Cuenta los registros en cada tabla"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tablas = cursor.fetchall()
            
            print("CONTEO DE REGISTROS:")
            print("-" * 40)
            for tabla in tablas:
                cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}")
                count = cursor.fetchone()[0]
                print(f"{tabla[0]}: {count} registros")
            print()
    
    def mostrar_requests_recientes(self, limite=3):
        """Muestra los requests más recientes"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT id, timestamp, device_id, processed_at, archived 
                FROM sensor_requests 
                ORDER BY processed_at DESC 
                LIMIT ?
            '''
            cursor.execute(query, (limite,))
            resultados = cursor.fetchall()
            
            print("ULTIMOS REQUESTS (JSON ORIGINALES):")
            print("-" * 60)
            for row in resultados:
                print(f"ID: {row[0]}")
                print(f"  Timestamp: {row[1]}")
                print(f"  Dispositivo: {row[2]}")
                print(f"  Procesado: {row[3]}")
                print(f"  Archivado: {'Sí' if row[4] else 'No'}")
                print()
    
    def mostrar_responses_recientes(self, limite=3):
        """Muestra los responses más recientes"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT r.id, s.calidad_aire_pred, s.co2_nivel, s.temperature, 
                       s.humedad, s.created_at
                FROM sensor_responses s
                INNER JOIN sensor_requests r ON s.request_id = r.id
                ORDER BY s.created_at DESC 
                LIMIT ?
            '''
            cursor.execute(query, (limite,))
            resultados = cursor.fetchall()
            
            print("ULTIMAS PREDICCIONES (RESPONSES):")
            print("-" * 60)
            for row in resultados:
                print(f"Request ID: {row[0]}")
                print(f"  Calidad del aire: {row[1]}")
                print(f"  Nivel CO2: {row[2]}")
                print(f"  Temperatura: {row[3]}°C")
                print(f"  Humedad: {row[4]}%")
                print(f"  Fecha analisis: {row[5]}")
                print()
    
    def mostrar_detalle_request(self, request_id):
        """Muestra el detalle completo de un request"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT timestamp, device_id, request_data, processed_at
                FROM sensor_requests 
                WHERE id = ?
            ''', (request_id,))
            resultado = cursor.fetchone()
            
            if resultado:
                print(f"DETALLE REQUEST ID: {request_id}")
                print("-" * 60)
                print(f"Timestamp: {resultado[0]}")
                print(f"Dispositivo: {resultado[1]}")
                print(f"Procesado: {resultado[3]}")
                print("\nDATOS DEL SENSOR (JSON):")
                try:
                    datos_json = json.loads(resultado[2])
                    print(json.dumps(datos_json, indent=2, ensure_ascii=False))
                except:
                    print(resultado[2])
            else:
                print(f"No se encontro request con ID: {request_id}")
    
    def mostrar_resumen_calidad(self):
        """Muestra un resumen de la calidad del aire"""
        with self.conectar() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT calidad_aire_pred, COUNT(*) as cantidad,
                       AVG(temperature) as temp_promedio,
                       AVG(humedad) as humedad_promedio
                FROM sensor_responses
                GROUP BY calidad_aire_pred
                ORDER BY 
                    CASE calidad_aire_pred
                        WHEN 'Excelente' THEN 1
                        WHEN 'Buena' THEN 2
                        WHEN 'Moderada' THEN 3
                        WHEN 'Deficiente' THEN 4
                        WHEN 'Peligrosa' THEN 5
                        ELSE 6
                    END
            '''
            cursor.execute(query)
            resultados = cursor.fetchall()
            
            print("RESUMEN CALIDAD DEL AIRE:")
            print("-" * 60)
            print(f"{'Categoría':<15} {'Cantidad':<10} {'Temp (°C)':<12} {'Humedad (%)':<12}")
            print("-" * 60)
            
            total = 0
            for row in resultados:
                print(f"{row[0]:<15} {row[1]:<10} {row[2]:<12.2f} {row[3]:<12.2f}")
                total += row[1]
            
            print("-" * 60)
            print(f"{'TOTAL':<15} {total:<10}")
            print()
    
    def exportar_a_csv(self, archivo_salida='data/processed/resumen_calidad.csv'):
        """Exporta los datos a CSV para analisis externo"""
        with self.conectar() as conn:
            query = '''
                SELECT 
                    r.timestamp,
                    r.device_id,
                    s.calidad_aire_pred,
                    s.co2_nivel,
                    s.temperature,
                    s.humedad,
                    s.presion,
                    s.created_at
                FROM sensor_responses s
                INNER JOIN sensor_requests r ON s.request_id = r.id
                ORDER BY r.timestamp
            '''
            df = pd.read_sql_query(query, conn)
            
            archivo_completo = os.path.join(self.proyecto_root, archivo_salida)
            os.makedirs(os.path.dirname(archivo_completo), exist_ok=True)
            df.to_csv(archivo_completo, index=False, encoding='utf-8')
            
            print(f"Datos exportados a: {archivo_completo}")
            print(f"Registros exportados: {len(df)}")
            print()
    
    def menu_interactivo(self):
        """Menu interactivo para consultar la base de datos"""
        while True:
            print("\n" + "="*60)
            print("CONSULTOR BASE DE DATOS - CALIDAD DEL AIRE UPS")
            print("="*60)
            print("1. Mostrar tablas disponibles")
            print("2. Contar registros")
            print("3. Ver ultimos requests")
            print("4. Ver ultimas predicciones")
            print("5. Resumen calidad del aire")
            print("6. Ver detalle de un request")
            print("7. Exportar datos a CSV")
            print("8. Salir")
            print("-"*60)
            
            opcion = input("Seleccione una opcion (1-8): ").strip()
            
            if opcion == "1":
                self.mostrar_tablas()
            elif opcion == "2":
                self.contar_registros()
            elif opcion == "3":
                self.mostrar_requests_recientes()
            elif opcion == "4":
                self.mostrar_responses_recientes()
            elif opcion == "5":
                self.mostrar_resumen_calidad()
            elif opcion == "6":
                try:
                    request_id = int(input("Ingrese ID del request: "))
                    self.mostrar_detalle_request(request_id)
                except ValueError:
                    print("Error: Ingrese un numero valido")
            elif opcion == "7":
                self.exportar_a_csv()
            elif opcion == "8":
                print("Saliendo del consultor...")
                break
            else:
                print("Opcion invalida. Intente nuevamente.")
            
            input("\nPresione Enter para continuar...")

def main():
    """Funcion principal"""
    print("CONSULTOR BASE DE DATOS - SISTEMA DE CALIDAD DEL AIRE")
    print("="*60)
    
    consultor = ConsultaBaseDatos()
    
    # Verificar si la base de datos existe
    if not os.path.exists(consultor.db_path):
        print(f"Error: Base de datos no encontrada en {consultor.db_path}")
        print("Ejecute primero procesador_json.py para crear la base de datos")
        return
    
    # Menu interactivo
    consultor.menu_interactivo()

if __name__ == "__main__":
    main()
