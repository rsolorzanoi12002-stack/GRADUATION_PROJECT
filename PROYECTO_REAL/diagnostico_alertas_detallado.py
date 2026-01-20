# verificar_contenido_tablas.py
import sqlite3
import json
import pandas as pd

def verificar_contenido_detallado():
    """Verifica contenido detallado de todas las tablas"""
    db_path = 'data/database/calidad_aire.db'
    conn = sqlite3.connect(db_path)
    
    print("=" * 80)
    print("VERIFICACIÓN DETALLADA DEL CONTENIDO DE TABLAS")
    print("=" * 80)
    
    # 1. VERIFICAR ALERTAS SISTEMA
    print("\n1. TABLA ALERTAS_SISTEMA (últimas 20):")
    print("-" * 40)
    query = '''
        SELECT id, timestamp, nivel, tipo, ubicacion, mensaje, procesada, fecha_procesada
        FROM alertas_sistema 
        ORDER BY id DESC 
        LIMIT 20
    '''
    df_alertas = pd.read_sql_query(query, conn)
    print(df_alertas.to_string(index=False))
    
    # Estadísticas de alertas
    print(f"\nTotal alertas: {len(df_alertas)}")
    print(f"Alertas por nivel:\n{df_alertas['nivel'].value_counts()}")
    print(f"Alertas por tipo:\n{df_alertas['tipo'].value_counts()}")
    print(f"Alertas procesadas: {df_alertas['procesada'].sum()}/{len(df_alertas)}")
    
    # 2. VERIFICAR SENSOR_RESPONSES con CO2 crítico
    print("\n\n2. SENSOR_RESPONSES con CO2 elevado (últimas 15):")
    print("-" * 40)
    query = '''
        SELECT sr.id, sr.request_id, sr.calidad_aire_pred, sr.co2_nivel, 
               sr.temperature, sr.humedad, sr.prediccion_detalle
        FROM sensor_responses sr
        WHERE sr.co2_nivel LIKE '%Critico%' OR sr.co2_nivel LIKE '%Muy elevado%'
           OR sr.co2_nivel LIKE '%Elevado%'
        ORDER BY sr.id DESC 
        LIMIT 15
    '''
    df_respuestas = pd.read_sql_query(query, conn)
    print(df_respuestas[['id', 'request_id', 'co2_nivel', 'temperature', 'humedad']].to_string(index=False))
    
    # Analizar prediccion_detalle para ver CO2 ppm
    print("\nDetalles de CO2 en respuestas críticas:")
    for _, row in df_respuestas.iterrows():
        try:
            detalles = json.loads(row['prediccion_detalle'])
            co2_ppm = detalles.get('co2_ppm', 'N/A')
            recomendacion = detalles.get('recomendaciones', 'N/A')[:60]
            print(f"ID {row['id']}: CO2 {co2_ppm} ppm - {recomendacion}")
        except:
            print(f"ID {row['id']}: Error leyendo detalles")
    
    # 3. VERIFICAR CORRELACIÓN ENTRE RESPUESTAS Y ALERTAS
    print("\n\n3. CORRELACIÓN RESPUESTAS-ALERTAS:")
    print("-" * 40)
    
    # Buscar respuestas críticas que deberían tener alertas
    query = '''
        SELECT sr.id, sr.request_id, sr.calidad_aire_pred, sr.co2_nivel,
               sr.temperature, sr.humedad,
               (SELECT COUNT(*) FROM alertas_sistema a 
                WHERE a.timestamp >= datetime(sr.created_at, '-10 minutes')
                  AND a.timestamp <= datetime(sr.created_at, '+10 minutes')
                  AND a.nivel IN ('CRITICA', 'ALTA')) as alertas_cercanas
        FROM sensor_responses sr
        WHERE sr.co2_nivel LIKE '%Critico%' OR sr.co2_nivel LIKE '%Muy elevado%'
        ORDER BY sr.id DESC 
        LIMIT 10
    '''
    df_correlacion = pd.read_sql_query(query, conn)
    print(df_correlacion.to_string(index=False))
    
    # 4. VERIFICAR SENSOR_REQUESTS sin procesar
    print("\n\n4. REQUESTS SIN PROCESAR:")
    print("-" * 40)
    query = '''
        SELECT id, timestamp, device_id, 
               processed_at IS NULL as pendiente,
               archived
        FROM sensor_requests 
        WHERE processed_at IS NULL OR archived = 0
        ORDER BY id DESC 
        LIMIT 10
    '''
    df_pendientes = pd.read_sql_query(query, conn)
    print(df_pendientes.to_string(index=False))
    
    # 5. VERIFICAR ARCHIVOS_PROCESADOS
    print("\n\n5. ARCHIVOS_PROCESADOS (últimos 10):")
    print("-" * 40)
    query = '''
        SELECT id, nombre_archivo, fecha_procesado, procesado, request_id
        FROM archivos_procesados 
        ORDER BY id DESC 
        LIMIT 10
    '''
    df_archivos = pd.read_sql_query(query, conn)
    print(df_archivos.to_string(index=False))
    
    conn.close()
    
    # 6. RESUMEN FINAL
    print("\n" + "=" * 80)
    print("RESUMEN FINAL DEL DIAGNÓSTICO")
    print("=" * 80)
    
    total_respuestas = len(df_respuestas) if not df_respuestas.empty else 0
    total_alertas = len(df_alertas)
    respuestas_criticas = len(df_respuestas[df_respuestas['co2_nivel'].str.contains('Critico|Muy elevado')])
    
    print(f"Total respuestas críticas: {respuestas_criticas}")
    print(f"Total alertas generadas: {total_alertas}")
    print(f"Ratio alertas/respuestas críticas: {total_alertas/respuestas_criticas if respuestas_criticas > 0 else 'N/A'}")
    
    if respuestas_criticas > total_alertas:
        print(f"⚠️  PROBLEMA: Solo se generaron {total_alertas} alertas de {respuestas_criticas} respuestas críticas")
        print("   Posibles causas:")
        print("   1. Sistema de alertas no está detectando todos los casos")
        print("   2. Las alertas se generan pero no se guardan correctamente")
        print("   3. Umbrales de alerta mal configurados")
    
    return {
        'alertas': df_alertas,
        'respuestas_criticas': df_respuestas,
        'pendientes': df_pendientes
    }

def verificar_sistema_alertas():
    """Verifica específicamente el sistema de alertas"""
    print("\n" + "=" * 80)
    print("VERIFICACIÓN DEL SISTEMA DE ALERTAS")
    print("=" * 80)
    
    # Importar el sistema de alertas directamente
    import sys
    sys.path.append('scripts')
    
    try:
        from sistema_alertas import SistemaAlertas
        
        sistema = SistemaAlertas()
        print("✓ Sistema de alertas cargado correctamente")
        
        # Verificar configuración
        print("\nConfiguración del sistema de alertas:")
        if hasattr(sistema, 'umbrales'):
            print(f"  Umbrales: {sistema.umbrales}")
        if hasattr(sistema, 'config'):
            print(f"  Config: {sistema.config}")
        
        # Verificar métodos disponibles
        print("\nMétodos disponibles:")
        metodos = [m for m in dir(sistema) if not m.startswith('_')]
        for metodo in metodos:
            print(f"  - {metodo}")
            
    except Exception as e:
        print(f"✗ Error cargando sistema de alertas: {e}")
        
        # Intentar ver el archivo directamente
        try:
            with open('scripts/sistema_alertas.py', 'r', encoding='utf-8') as f:
                contenido = f.read()
                print(f"\nPrimeras 20 líneas del archivo:")
                for i, linea in enumerate(contenido.split('\n')[:20], 1):
                    print(f"  {i:2}: {linea}")
        except:
            print("No se pudo leer el archivo sistema_alertas.py")

if __name__ == "__main__":
    resultados = verificar_contenido_detallado()
    verificar_sistema_alertas()
    
    print("\n" + "=" * 80)
    print("RECOMENDACIONES INMEDIATAS:")
    print("=" * 80)
    print("1. Ejecuta este script para ver el contenido real de las tablas")
    print("2. Comparte los resultados (especialmente la tabla de alertas)")
    print("3. Revisa el archivo 'scripts/sistema_alertas.py'")
    print("4. Verifica los umbrales de CO2 en la configuración")