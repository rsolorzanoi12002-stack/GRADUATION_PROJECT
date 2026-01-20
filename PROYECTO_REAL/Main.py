"""
SISTEMA PRINCIPAL DE MONITOREO DE CALIDAD DEL AIRE
Universidad Politécnica Salesiana - Campus Centenario Guayaquil
Autores: Roberth Solórzano - Carlos Rojas
Versión: 4.0 - Sincronizado con procesador corregido
"""

import os
import sys
import time
from datetime import datetime
import json

def mostrar_banner():
    """Muestra el banner del sistema"""
    print("\n" + "="*70)
    print("UNIVERSIDAD POLITECNICA SALESIANA - CAMPUS CENTENARIO")
    print("SISTEMA DE MONITOREO DE CALIDAD DEL AIRE - VERSIÓN 4.0")
    print("="*70)
    print("Proyecto de Titulacion - Ingenieria en Ciencias de la Computacion")
    print("Autores: Roberth Andres Solorzano Ibarra")
    print("         Carlos Francisco Rojas Cabello")
    print("Tutor: Msc. Jaime Carriel Jose Roberto")
    print("="*70)
    print(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*70 + "\n")

def verificar_estructura():
    """Verifica que la estructura del proyecto sea correcta"""
    print("="*50)
    print("VERIFICANDO ESTRUCTURA DEL PROYECTO")
    print("="*50)
    
    directorios_requeridos = [
        'config',
        'data/raw_json',
        'data/processed',
        'data/database',
        'data/archive',
        'data/alertas',
        'scripts',
        'models',
        'reports',
        'logs'
    ]
    
    todos_ok = True
    for directorio in directorios_requeridos:
        if os.path.exists(directorio):
            print(f"  OK - {directorio}")
        else:
            print(f"  NO - {directorio} (creando...)")
            try:
                os.makedirs(directorio, exist_ok=True)
                print(f"    -> Creado exitosamente")
            except Exception as e:
                print(f"    -> Error creando: {e}")
                todos_ok = False
    
    archivos_requeridos = [
        'config/config.json',
        'scripts/procesador_json.py',
        'scripts/consulta_db.py',
        'scripts/dashboard.py',
        'scripts/modelo_mejorado.py',
        'scripts/sistema_alertas.py'
    ]
    
    for archivo in archivos_requeridos:
        if os.path.exists(archivo):
            print(f"  OK - {archivo}")
        else:
            print(f"  NO - {archivo}")
            # Si es config.json, crear uno por defecto
            if archivo == 'config/config.json':
                try:
                    os.makedirs('config', exist_ok=True)
                    config_default = {
                        "paths": {
                            "input_dir": "data/raw_json",
                            "output_dir": "data/processed",
                            "database": "data/database/calidad_aire.db",
                            "archive": "data/archive"
                        },
                        "model": {
                            "path": "models/random_forest_model.pkl",
                            "version": "2.0"
                        },
                        "alertas": {
                            "co2_critico": 1200,
                            "co2_alto": 800,
                            "temperatura_alta": 30,
                            "humedad_alta": 80
                        }
                    }
                    with open('config/config.json', 'w') as f:
                        json.dump(config_default, f, indent=2)
                    print(f"    -> Creado config.json por defecto")
                except Exception as e:
                    print(f"    -> Error creando config.json: {e}")
                    todos_ok = False
            else:
                todos_ok = False
    
    print("="*50)
    print("ESTRUCTURA: " + ("COMPLETA Y CORRECTA" if todos_ok else "CON FALTANTES"))
    print("="*50)
    
    return todos_ok

def verificar_dependencias():
    """Verifica que las dependencias esten instaladas"""
    print("\n" + "="*50)
    print("VERIFICANDO DEPENDENCIAS")
    print("="*50)
    
    dependencias = [
        ('pandas', 'pandas'),
        ('sklearn', 'scikit-learn'),
        ('matplotlib', 'matplotlib'),
        ('joblib', 'joblib'),
        ('numpy', 'numpy'),
        ('sqlite3', 'sqlite3 (built-in)'),
        ('seaborn', 'seaborn')
    ]
    
    todas_ok = True
    for import_name, nombre_mostrar in dependencias:
        try:
            __import__(import_name)
            print(f"  OK - {nombre_mostrar}")
        except ImportError:
            if nombre_mostrar == 'sqlite3 (built-in)':
                print(f"  NO - {nombre_mostrar} (ERROR: Python sin sqlite3)")
                todas_ok = False
            else:
                print(f"  NO - {nombre_mostrar}")
                print(f"      Ejecutar: pip install {nombre_mostrar.split()[0]}")
                todas_ok = False
    
    print("="*50)
    print("DEPENDENCIAS: " + ("TODAS INSTALADAS" if todas_ok else "FALTAN INSTALAR"))
    print("="*50)
    return todas_ok

def ejecutar_procesador():
    """Ejecuta el procesador de JSON - VERSIÓN CORREGIDA"""
    print("\n" + "="*50)
    print("EJECUTANDO PROCESADOR DE DATOS - VERSIÓN 4.0")
    print("="*50)
    
    # Verificar si hay archivos para procesar
    raw_json_dir = 'data/raw_json'
    if not os.path.exists(raw_json_dir):
        print("ERROR: Directorio data/raw_json no existe")
        return False
    
    archivos_json = [f for f in os.listdir(raw_json_dir) if f.endswith('.json')]
    if not archivos_json:
        print("ADVERTENCIA: No hay archivos JSON para procesar")
        print("  Los archivos ya procesados se encuentran en: data/archive/")
        print("  Coloque nuevos archivos JSON en data/raw_json/")
        return False
    
    print(f"Archivos encontrados para procesar: {len(archivos_json)}")
    print("Ejemplos:")
    for archivo in archivos_json[:3]:  # Mostrar primeros 3
        print(f"  * {archivo}")
    if len(archivos_json) > 3:
        print(f"  ... y {len(archivos_json)-3} más")
    
    # Agregar scripts al path
    sys.path.append('scripts')
    
    try:
        # Importar y ejecutar procesador
        print("\nIniciando procesamiento de datos...")
        print("-"*40)
        
        from procesador_json import main as procesador_main
        procesador_main()
        
        print("\n" + "="*50)
        print("PROCESADOR EJECUTADO EXITOSAMENTE")
        print("="*50)
        
        # Verificar resultados
        print("\nVERIFICANDO RESULTADOS:")
        print("-"*40)
        
        # 1. Verificar archivos en archive
        archive_dir = 'data/archive'
        if os.path.exists(archive_dir):
            archivos_archivados = [f for f in os.listdir(archive_dir) if f.endswith('.json')]
            print(f"  Archivos movidos a archive: {len(archivos_archivados)}")
        
        # 2. Verificar base de datos
        db_path = 'data/database/calidad_aire.db'
        if os.path.exists(db_path):
            import sqlite3
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Verificar registros en sensor_requests
                cursor.execute("SELECT COUNT(*) FROM sensor_requests WHERE processed_at IS NOT NULL")
                procesados = cursor.fetchone()[0]
                print(f"  Requests procesados en BD: {procesados}")
                
                # Verificar sensor_responses
                cursor.execute("SELECT COUNT(*) FROM sensor_responses")
                responses = cursor.fetchone()[0]
                print(f"  Responses guardados: {responses}")
                
                # Verificar calidad del aire procesada
                cursor.execute("SELECT calidad_aire_pred, COUNT(*) FROM sensor_responses GROUP BY calidad_aire_pred")
                distribucion = cursor.fetchall()
                if distribucion:
                    print(f"  Distribución calidad del aire:")
                    for calidad, cantidad in distribucion:
                        print(f"    * {calidad}: {cantidad}")
                
                conn.close()
            except Exception as e:
                print(f"  Error consultando BD: {e}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR EJECUTANDO PROCESADOR: {e}")
        import traceback
        traceback.print_exc()
        return False

def ejecutar_dashboard():
    """Ejecuta el dashboard de visualizacion"""
    print("\n" + "="*50)
    print("EJECUTANDO DASHBOARD DE VISUALIZACION")
    print("="*50)
    
    try:
        # Primero verificar si hay datos
        db_path = 'data/database/calidad_aire.db'
        if not os.path.exists(db_path):
            print("ERROR: Base de datos no encontrada")
            print("  Ejecute primero el procesador (opcion 2)")
            return False
        
        # Verificar si hay datos en sensor_responses
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sensor_responses")
        count = cursor.fetchone()[0]
        conn.close()
        
        if count == 0:
            print("ADVERTENCIA: No hay datos procesados en la base de datos")
            print("  Ejecute primero el procesador (opcion 2)")
            return False
        
        print(f"  Datos disponibles: {count} registros")
        
        # Importar y ejecutar
        sys.path.append('scripts')
        from dashboard import main as dashboard_main
        dashboard_main()
        return True
    except Exception as e:
        print(f"✗ ERROR EJECUTANDO DASHBOARD: {e}")
        return False

def ejecutar_consulta():
    """Ejecuta el consultor de base de datos"""
    print("\n" + "="*50)
    print("EJECUTANDO CONSULTOR DE BASE DE DATOS")
    print("="*50)
    
    try:
        # Verificar si existe la base de datos
        db_path = 'data/database/calidad_aire.db'
        if not os.path.exists(db_path):
            print("ERROR: Base de datos no encontrada")
            print("  Ejecute primero el procesador (opcion 2)")
            return False
        
        # Verificar si hay datos
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Listar tablas disponibles
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        
        print(f"\nTablas disponibles en la base de datos:")
        for tabla in tablas:
            tabla_nombre = tabla[0]
            cursor.execute(f"SELECT COUNT(*) FROM {tabla_nombre}")
            count = cursor.fetchone()[0]
            print(f"  * {tabla_nombre}: {count} registros")
        
        conn.close()
        
        # Importar y ejecutar
        sys.path.append('scripts')
        from consulta_db import main as consulta_main
        consulta_main()
        return True
    except Exception as e:
        print(f"✗ ERROR EJECUTANDO CONSULTOR: {e}")
        return False

def ejecutar_dashboard_alertas():
    """Ejecuta el dashboard de alertas"""
    print("\n" + "="*50)
    print("EJECUTANDO DASHBOARD DE ALERTAS")
    print("="*50)
    
    try:
        sys.path.append('scripts')
        
        # Primero verificar si hay alertas en la base de datos
        db_path = 'data/database/calidad_aire.db'
        if os.path.exists(db_path):
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Verificar si existe la tabla alertas_sistema
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alertas_sistema';")
            tabla_existe = cursor.fetchone()
            
            if tabla_existe:
                cursor.execute("SELECT COUNT(*) FROM alertas_sistema")
                total_alertas = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM alertas_sistema WHERE procesada = 0")
                alertas_pendientes = cursor.fetchone()[0]
                
                print(f"\nAlertas en base de datos:")
                print(f"  * Total alertas: {total_alertas}")
                print(f"  * Pendientes (procesada=0): {alertas_pendientes}")
                print(f"  * Procesadas: {total_alertas - alertas_pendientes}")
                
                # Mostrar últimas alertas
                if total_alertas > 0:
                    cursor.execute("""
                        SELECT nivel, tipo, ubicacion, mensaje, timestamp 
                        FROM alertas_sistema 
                        ORDER BY timestamp DESC 
                        LIMIT 5
                    """)
                    ultimas_alertas = cursor.fetchall()
                    
                    print(f"\nÚltimas 5 alertas:")
                    for alerta in ultimas_alertas:
                        nivel, tipo, ubicacion, mensaje, timestamp = alerta
                        print(f"  * [{nivel}] {tipo} - {ubicacion}: {mensaje[:50]}... ({timestamp})")
            
            conn.close()
        
        # Intentar importar dashboard_alertas si existe
        try:
            from alertas_dashboard import main as alertas_main
            alertas_main()
            return True
        except ImportError:
            print("\nDashboard de alertas no disponible como módulo separado.")
            print("Mostrando información de alertas desde la base de datos...")
            return True
                
    except Exception as e:
        print(f"✗ ERROR EJECUTANDO DASHBOARD DE ALERTAS: {e}")
        return False

def generar_reporte_alertas():
    """Genera reporte de alertas"""
    print("\n" + "="*50)
    print("GENERANDO REPORTE DE ALERTAS (ÚLTIMAS 24 HORAS)")
    print("="*50)
    
    try:
        # Primero verificar base de datos
        db_path = 'data/database/calidad_aire.db'
        if not os.path.exists(db_path):
            print("ERROR: Base de datos no encontrada")
            print("  Ejecute primero el procesador (opcion 2)")
            return False
        
        import sqlite3
        from datetime import datetime, timedelta
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Calcular fecha de hace 24 horas
        fecha_limite = (datetime.now() - timedelta(hours=24)).isoformat()
        
        # Consultar alertas de las últimas 24 horas
        cursor.execute("""
            SELECT nivel, tipo, ubicacion, mensaje, timestamp, procesada
            FROM alertas_sistema 
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
        """, (fecha_limite,))
        
        alertas = cursor.fetchall()
        
        print(f"\nAlertas en las últimas 24 horas: {len(alertas)}")
        print("-"*60)
        
        if alertas:
            # Contadores por nivel
            niveles = {}
            tipos = {}
            ubicaciones = {}
            
            for alerta in alertas:
                nivel, tipo, ubicacion, mensaje, timestamp, procesada = alerta
                
                # Contar niveles
                niveles[nivel] = niveles.get(nivel, 0) + 1
                
                # Contar tipos
                tipos[tipo] = tipos.get(tipo, 0) + 1
                
                # Contar ubicaciones
                if ubicacion:
                    ubicaciones[ubicacion] = ubicaciones.get(ubicacion, 0) + 1
            
            print("\nRESUMEN ESTADÍSTICO:")
            print(f"  Total alertas: {len(alertas)}")
            
            print("\n  Por nivel de severidad:")
            for nivel, count in sorted(niveles.items(), key=lambda x: x[1], reverse=True):
                porcentaje = (count / len(alertas)) * 100
                print(f"    * {nivel}: {count} ({porcentaje:.1f}%)")
            
            print("\n  Por tipo de alerta:")
            for tipo, count in sorted(tipos.items(), key=lambda x: x[1], reverse=True):
                porcentaje = (count / len(alertas)) * 100
                print(f"    * {tipo}: {count} ({porcentaje:.1f}%)")
            
            print("\n  Por ubicación:")
            for ubicacion, count in sorted(ubicaciones.items(), key=lambda x: x[1], reverse=True)[:5]:
                porcentaje = (count / len(alertas)) * 100
                print(f"    * {ubicacion}: {count} ({porcentaje:.1f}%)")
            
            # Guardar reporte
            reporte_path = 'reports/reporte_alertas.json'
            os.makedirs('reports', exist_ok=True)
            
            reporte = {
                'fecha_generacion': datetime.now().isoformat(),
                'periodo': '24_horas',
                'total_alertas': len(alertas),
                'resumen_niveles': niveles,
                'resumen_tipos': tipos,
                'resumen_ubicaciones': ubicaciones,
                'detalle_alertas': [
                    {
                        'nivel': nivel,
                        'tipo': tipo,
                        'ubicacion': ubicacion,
                        'mensaje': mensaje,
                        'timestamp': timestamp,
                        'procesada': bool(procesada)
                    }
                    for nivel, tipo, ubicacion, mensaje, timestamp, procesada in alertas[:20]  # Limitar a 20
                ]
            }
            
            with open(reporte_path, 'w', encoding='utf-8') as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            
            print(f"\nReporte guardado en: {reporte_path}")
            
        else:
            print("No hay alertas en las últimas 24 horas")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ ERROR GENERANDO REPORTE DE ALERTAS: {e}")
        return False

def entrenar_modelo():
    """Entrena o actualiza el modelo de ML"""
    print("\n" + "="*50)
    print("ENTRENANDO MODELO DE MACHINE LEARNING")
    print("="*50)
    
    try:
        # Verificar si hay datos en la base de datos
        db_path = 'data/database/calidad_aire.db'
        if not os.path.exists(db_path):
            print("ERROR: Base de datos no encontrada")
            print("  Ejecute primero el procesador (opcion 2)")
            return False
        
        # Verificar si hay suficientes datos en sensor_responses
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sensor_responses")
        count = cursor.fetchone()[0]
        conn.close()
        
        if count < 10:
            print(f"ADVERTENCIA: Solo hay {count} registros en la base de datos")
            print("  Se recomienda al menos 50 registros para un buen entrenamiento")
            print("  ¿Desea continuar de todos modos? (s/N)")
            respuesta = input().strip().lower()
            if respuesta != 's':
                print("Entrenamiento cancelado")
                return False
        
        sys.path.append('scripts')
        from modelo_mejorado import main as modelo_main
        modelo_main()
        return True
    except Exception as e:
        print(f"✗ ERROR ENTRENANDO MODELO: {e}")
        return False

def mostrar_estado_sistema():
    """Muestra el estado actual del sistema - VERSIÓN ACTUALIZADA"""
    print("\n" + "="*50)
    print("ESTADO DEL SISTEMA - VERSIÓN 4.0")
    print("="*50)
    
    # 1. Verificar archivos procesados
    print("\n1. ARCHIVOS JSON:")
    print("-"*40)
    
    raw_json_dir = 'data/raw_json'
    archive_dir = 'data/archive'
    
    if os.path.exists(raw_json_dir):
        archivos_raw = [f for f in os.listdir(raw_json_dir) if f.endswith('.json')]
        print(f"  Pendientes de procesar: {len(archivos_raw)}")
        if archivos_raw:
            print(f"    Ejemplos: {', '.join(archivos_raw[:3])}")
            if len(archivos_raw) > 3:
                print(f"    ... y {len(archivos_raw)-3} más")
    else:
        print("  Pendientes de procesar: Directorio no existe")
    
    if os.path.exists(archive_dir):
        archivos_archivados = [f for f in os.listdir(archive_dir) if f.endswith('.json')]
        print(f"  Procesados (en archive): {len(archivos_archivados)}")
    else:
        print("  Procesados: Directorio no existe")
    
    # 2. Verificar base de datos (CON INFORMACIÓN ACTUALIZADA)
    print("\n2. BASE DE DATOS (calidad_aire.db):")
    print("-"*40)
    
    db_path = 'data/database/calidad_aire.db'
    if os.path.exists(db_path):
        import sqlite3
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Verificar tablas específicas del sistema corregido
            tablas_esperadas = ['sensor_requests', 'sensor_responses', 'alertas_sistema', 'archivos_procesados']
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tablas_db = [t[0] for t in cursor.fetchall()]
            
            print(f"  Ubicacion: {db_path}")
            print(f"  Tablas encontradas: {len(tablas_db)}")
            
            # Contar registros en cada tabla IMPORTANTE
            for tabla in tablas_esperadas:
                if tabla in tablas_db:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                        count = cursor.fetchone()[0]
                        print(f"    * {tabla}: {count} registros")
                    except:
                        print(f"    * {tabla}: Error al contar")
                else:
                    print(f"    * {tabla}: NO EXISTE")
            
            # Información específica de cada tabla
            try:
                # sensor_requests - estado de procesamiento
                cursor.execute("SELECT COUNT(*) FROM sensor_requests WHERE processed_at IS NULL")
                pendientes = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM sensor_requests WHERE processed_at IS NOT NULL")
                procesados = cursor.fetchone()[0]
                print(f"\n    sensor_requests: {pendientes} pendientes, {procesados} procesados")
                
                # alertas_sistema - estado de alertas
                cursor.execute("SELECT COUNT(*) FROM alertas_sistema WHERE procesada = 0")
                alertas_pendientes = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM alertas_sistema WHERE procesada = 1")
                alertas_procesadas = cursor.fetchone()[0]
                print(f"    alertas_sistema: {alertas_pendientes} pendientes, {alertas_procesadas} procesadas")
                
                # Calidad del aire actual
                cursor.execute("""
                    SELECT calidad_aire_pred, COUNT(*) 
                    FROM sensor_responses 
                    GROUP BY calidad_aire_pred 
                    ORDER BY COUNT(*) DESC
                    LIMIT 3
                """)
                top_calidad = cursor.fetchall()
                if top_calidad:
                    print(f"\n    Calidad del aire más frecuente:")
                    for calidad, cantidad in top_calidad:
                        print(f"      * {calidad}: {cantidad}")
                
            except Exception as e:
                print(f"    Error obteniendo detalles: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"  Error consultando BD: {e}")
    else:
        print("  No existe (ejecutar primero el procesador - opción 2)")
    
    # 3. Verificar modelos
    print("\n3. MODELOS DE MACHINE LEARNING:")
    print("-"*40)
    
    modelos_dir = 'models'
    if os.path.exists(modelos_dir):
        modelos_pkl = [f for f in os.listdir(modelos_dir) if f.endswith('.pkl')]
        modelos_json = [f for f in os.listdir(modelos_dir) if f.endswith('.json')]
        
        print(f"  Modelos entrenados (.pkl): {len(modelos_pkl)}")
        for modelo in modelos_pkl[:3]:
            ruta = os.path.join(modelos_dir, modelo)
            tamano = os.path.getsize(ruta) / 1024  # KB
            print(f"    * {modelo} ({tamano:.1f} KB)")
        if len(modelos_pkl) > 3:
            print(f"    ... y {len(modelos_pkl)-3} mas")
        
        print(f"  Metadata (.json): {len(modelos_json)}")
    else:
        print("  Directorio no existe")
    
    # 4. Verificar reportes
    print("\n4. REPORTES GENERADOS:")
    print("-"*40)
    
    reports_dir = 'reports'
    if os.path.exists(reports_dir):
        reportes_json = [f for f in os.listdir(reports_dir) if f.endswith('.json')]
        reportes_png = [f for f in os.listdir(reports_dir) if f.endswith('.png')]
        
        print(f"  Reportes JSON: {len(reportes_json)}")
        print(f"  Dashboard PNG: {len(reportes_png)}")
        
        # Mostrar los más recientes
        if reportes_json:
            reportes_json.sort(key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)), reverse=True)
            print(f"  Últimos reportes JSON:")
            for reporte in reportes_json[:2]:
                fecha = datetime.fromtimestamp(os.path.getmtime(os.path.join(reports_dir, reporte)))
                print(f"    * {reporte} ({fecha.strftime('%d/%m/%Y %H:%M')})")
        
        if reportes_png:
            reportes_png.sort(key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)), reverse=True)
            print(f"  Últimos dashboards PNG:")
            for dashboard in reportes_png[:2]:
                fecha = datetime.fromtimestamp(os.path.getmtime(os.path.join(reports_dir, dashboard)))
                print(f"    * {dashboard} ({fecha.strftime('%d/%m/%Y %H:%M')})")
    else:
        print("  Directorio no existe")
    
    # 5. Estado de variables de procesamiento
    print("\n5. VARIABLES DE PROCESAMIENTO:")
    print("-"*40)
    
    if os.path.exists(db_path):
        import sqlite3
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Verificar archivos_procesados
            cursor.execute("SELECT COUNT(*) FROM archivos_procesados WHERE procesado = 1")
            archivos_procesados = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM archivos_procesados WHERE procesado = 0")
            archivos_pendientes = cursor.fetchone()[0]
            print(f"  archivos_procesados.procesado: {archivos_procesados} en 1, {archivos_pendientes} en 0")
            
            # Verificar última fecha de procesamiento
            cursor.execute("SELECT MAX(fecha_procesado) FROM archivos_procesados")
            ultima_fecha = cursor.fetchone()[0]
            if ultima_fecha:
                print(f"  Última fecha procesado: {ultima_fecha}")
            
            # Verificar alertas procesadas
            cursor.execute("SELECT COUNT(*) FROM alertas_sistema WHERE procesada = 1 AND fecha_procesada IS NOT NULL")
            alertas_completas = cursor.fetchone()[0]
            print(f"  alertas_sistema completas (procesada=1, fecha_procesada!=NULL): {alertas_completas}")
            
            conn.close()
        except:
            print("  Error consultando variables de procesamiento")
    else:
        print("  Base de datos no disponible")
    
    print("\n" + "="*50)

def limpiar_datos_temporales():
    """Limpia datos temporales del sistema"""
    print("\n" + "="*50)
    print("LIMPIANDO DATOS TEMPORALES")
    print("="*50)
    
    print("ADVERTENCIA: Esta accion eliminara datos no procesados.")
    print("\nSe eliminaran los siguientes datos:")
    print("  [X] Archivos JSON en data/raw_json/ (pendientes de procesar)")
    print("  [X] Archivos en data/archive/ (ya procesados)")
    print("  [X] Alertas JSON en data/alertas/ (si existen)")
    print("  [X] Reportes/imagenes antiguos en reports/ (excepto 5 mas recientes)")
    
    print("\nSe MANTENDRAN los siguientes datos CRÍTICOS:")
    print("  [√] Base de datos (calidad_aire.db) - TODOS LOS DATOS PROCESADOS")
    print("  [√] Modelos entrenados (.pkl, .json)")
    print("  [√] Configuracion (.json)")
    print("  [√] Logs del sistema (.log)")
    
    print("\nIMPORTANTE: Los datos en la base de datos NO se eliminarán.")
    print("Los archivos en archive/ son solo copias de seguridad.")
    
    confirmar = input("\n¿Esta absolutamente seguro de continuar? (s/N): ").strip().lower()
    
    if confirmar != 's':
        print("Operacion cancelada por el usuario")
        return False
    
    confirmar2 = input("Escriba 'LIMPIAR' para proceder: ").strip().upper()
    
    if confirmar2 != 'LIMPIAR':
        print("Confirmacion incorrecta. Operacion cancelada.")
        return False
    
    try:
        import shutil
        
        directorios_a_limpiar = [
            'data/raw_json',
            'data/archive',
            'data/alertas'
        ]
        
        total_eliminados = 0
        
        for directorio in directorios_a_limpiar:
            if os.path.exists(directorio):
                # Contar archivos antes de eliminar
                archivos = [f for f in os.listdir(directorio) if f.endswith('.json')]
                # Eliminar archivos
                for archivo in archivos:
                    try:
                        os.remove(os.path.join(directorio, archivo))
                        total_eliminados += 1
                    except Exception as e:
                        print(f"  Error eliminando {archivo}: {e}")
                print(f"  Limpiados {len(archivos)} archivos de {directorio}")
            else:
                print(f"  Directorio {directorio} no existe, saltando...")
        
        # Limpiar reportes/imagenes antiguas (mantener ultimos 5 de cada tipo)
        reports_dir = 'reports'
        if os.path.exists(reports_dir):
            # Archivos JSON
            archivos_json = [f for f in os.listdir(reports_dir) if f.endswith('.json')]
            archivos_json.sort(key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)), reverse=True)
            
            if len(archivos_json) > 5:
                eliminar_json = archivos_json[5:]
                for archivo in eliminar_json:
                    try:
                        os.remove(os.path.join(reports_dir, archivo))
                        total_eliminados += 1
                    except:
                        pass
                print(f"  Mantenidos 5 reportes JSON, eliminados {len(eliminar_json)} antiguos")
            else:
                print(f"  Mantenidos {len(archivos_json)} reportes JSON")
            
            # Archivos PNG
            archivos_png = [f for f in os.listdir(reports_dir) if f.endswith('.png')]
            archivos_png.sort(key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)), reverse=True)
            
            if len(archivos_png) > 5:
                eliminar_png = archivos_png[5:]
                for archivo in eliminar_png:
                    try:
                        os.remove(os.path.join(reports_dir, archivo))
                        total_eliminados += 1
                    except:
                        pass
                print(f"  Mantenidos 5 dashboards PNG, eliminados {len(eliminar_png)} antiguos")
            else:
                print(f"  Mantenidos {len(archivos_png)} dashboards PNG")
        
        print(f"\n" + "="*50)
        print(f"LIMPIEZA COMPLETADA")
        print(f"Total archivos eliminados: {total_eliminados}")
        print("="*50)
        print("\nNOTA: La base de datos con todos los datos procesados se mantiene intacta.")
        print("      Puede ejecutar 'Mostrar estado del sistema' para verificar.")
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR DURANTE LA LIMPIEZA: {e}")
        return False

def ejecutar_flujo_completo():
    """Ejecuta el flujo completo del sistema"""
    print("\n" + "="*70)
    print("EJECUTANDO FLUJO COMPLETO DEL SISTEMA - VERSIÓN 4.0")
    print("="*70)
    
    # Verificar archivos pendientes
    raw_json_dir = 'data/raw_json'
    archivos_pendientes = 0
    if os.path.exists(raw_json_dir):
        archivos_pendientes = len([f for f in os.listdir(raw_json_dir) if f.endswith('.json')])
    
    print(f"\nESTADO INICIAL:")
    print(f"  Archivos JSON pendientes: {archivos_pendientes}")
    
    if archivos_pendientes == 0:
        print("\nADVERTENCIA: No hay archivos JSON para procesar")
        print("Coloque archivos JSON en data/raw_json/ o continúe con el resto del flujo")
        print("¿Desea continuar con el resto del flujo? (s/N)")
        respuesta = input().strip().lower()
        if respuesta != 's':
            print("Flujo cancelado por el usuario")
            return
    
    resultados = []
    errores = []
    
    # PASO 1: Procesar datos
    print("\n" + "-"*30)
    print("PASO 1: PROCESANDO DATOS")
    print("-"*30)
    if ejecutar_procesador():
        resultados.append("Procesador ejecutado correctamente")
    else:
        resultados.append("✗ Error en procesador")
        errores.append("Procesador falló")
        print("Flujo interrumpido por error en procesador")
        # Preguntar si continuar
        print("\n¿Desea continuar con el resto del flujo? (s/N)")
        respuesta = input().strip().lower()
        if respuesta != 's':
            return
    
    time.sleep(1)
    
    # PASO 2: Entrenar modelo
    print("\n" + "-"*30)
    print("PASO 2: ENTRENANDO MODELO")
    print("-"*30)
    if entrenar_modelo():
        resultados.append("Modelo entrenado correctamente")
    else:
        resultados.append("✗ Error entrenando modelo")
        errores.append("Entrenamiento falló")
    
    time.sleep(1)
    
    # PASO 3: Generar dashboard
    print("\n" + "-"*30)
    print("PASO 3: GENERANDO DASHBOARD")
    print("-"*30)
    if ejecutar_dashboard():
        resultados.append("Dashboard generado correctamente")
    else:
        resultados.append("✗ Error generando dashboard")
        errores.append("Dashboard falló")
    
    time.sleep(1)
    
    # PASO 4: Generar reporte de alertas
    print("\n" + "-"*30)
    print("PASO 4: GENERANDO REPORTE DE ALERTAS")
    print("-"*30)
    if generar_reporte_alertas():
        resultados.append("Reporte de alertas generado")
    else:
        resultados.append("✗ Error generando reporte")
        errores.append("Reporte falló")
    
    # RESUMEN FINAL
    print("\n" + "="*70)
    print("RESUMEN DEL FLUJO COMPLETO")
    print("="*70)
    for resultado in resultados:
        print(f"  {resultado}")
    
    if errores:
        print(f"\nErrores encontrados: {len(errores)}")
        for error in errores:
            print(f"  * {error}")
    
    # Verificar estado final
    print("\nESTADO FINAL DEL SISTEMA:")
    print("-"*40)
    
    # Archivos procesados
    if os.path.exists('data/archive'):
        archivos_procesados = len([f for f in os.listdir('data/archive') if f.endswith('.json')])
        print(f"  Archivos procesados: {archivos_procesados}")
    
    # Base de datos
    db_path = 'data/database/calidad_aire.db'
    if os.path.exists(db_path):
        import sqlite3
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sensor_responses")
            count = cursor.fetchone()[0]
            conn.close()
            print(f"  Datos en base de datos: {count} registros")
        except:
            print(f"  Base de datos: PRESENTE (error al consultar)")
    
    # Modelos
    if os.path.exists('models'):
        modelos = len([f for f in os.listdir('models') if f.endswith('.pkl')])
        print(f"  Modelos disponibles: {modelos}")
    
    print("="*70)

def menu_principal():
    """Menu principal del sistema"""
    # Agregar scripts al path para evitar problemas de importacion
    sys.path.insert(0, 'scripts')
    
    while True:
        print("\n" + "="*70)
        print("MENU PRINCIPAL - SISTEMA DE MONITOREO V4.0")
        print("="*70)
        print("1.  Verificar estructura y dependencias")
        print("2.  Procesar datos de sensores")
        print("3.  Visualizar dashboard de calidad del aire")
        print("4.  Consultar base de datos")
        print("5.  Ver dashboard de alertas")
        print("6.  Generar reporte de alertas")
        print("7.  Entrenar/actualizar modelo de ML")
        print("8.  Mostrar estado del sistema")
        print("9.  Ejecutar flujo completo (2->7->3->6)")
        print("10. Limpiar datos temporales")
        print("11. Salir del sistema")
        
        try:
            opcion = input("\nSeleccione una opcion (1-11): ").strip()
            
            if opcion == "1":
                verificar_estructura()
                verificar_dependencias()
            
            elif opcion == "2":
                if ejecutar_procesador():
                    print("\n" + "="*50)
                    print("PROCESAMIENTO COMPLETADO EXITOSAMENTE")
                    print("  Variables actualizadas:")
                    print("    - sensor_requests.processed_at")
                    print("    - archivos_procesados.procesado / fecha_procesado")
                    print("    - alertas_sistema.procesada / fecha_procesada")
                    print("="*50)
                else:
                    print("\n" + "="*50)
                    print("✗ ERROR EN EL PROCESAMIENTO")
                    print("  Revise los mensajes de error anteriores")
                    print("="*50)
            
            elif opcion == "3":
                if ejecutar_dashboard():
                    print("\nDashboard ejecutado correctamente")
                else:
                    print("\n✗ Error ejecutando dashboard")
            
            elif opcion == "4":
                ejecutar_consulta()
            
            elif opcion == "5":
                ejecutar_dashboard_alertas()
            
            elif opcion == "6":
                if generar_reporte_alertas():
                    print("\nReporte generado exitosamente")
                else:
                    print("\n✗ Error generando reporte")
            
            elif opcion == "7":
                if entrenar_modelo():
                    print("\nModelo entrenado exitosamente")
                else:
                    print("\n✗ Error entrenando modelo")
            
            elif opcion == "8":
                mostrar_estado_sistema()
            
            elif opcion == "9":
                ejecutar_flujo_completo()
            
            elif opcion == "10":
                limpiar_datos_temporales()
            
            elif opcion == "11":
                print("\n" + "="*70)
                print("SALIENDO DEL SISTEMA")
                print("Gracias por usar el Sistema de Monitoreo de Calidad del Aire")
                print("UPS Campus Centenario Guayaquil - Versión 4.0")
                print("="*70)
                break
            
            else:
                print("\n✗ Opcion invalida. Intente nuevamente.")
        
        except KeyboardInterrupt:
            print("\n\nOperacion interrumpida por el usuario")
            continue
        except Exception as e:
            print(f"\n✗ Error inesperado: {e}")
            import traceback
            traceback.print_exc()
        
        if opcion != "11":
            input("\nPresione Enter para continuar...")

def main():
    """Funcion principal"""
    # Mostrar banner
    mostrar_banner()
    
    # Verificar si estamos en el directorio correcto
    if not os.path.exists('scripts'):
        print("ERROR: No se encuentra la carpeta 'scripts'")
        print("Ejecute este archivo desde la raiz del proyecto")
        print("Directorio actual:", os.getcwd())
        print("\nLa estructura debe ser:")
        print("  PROYECTO_REAL/")
        print("    ├── Main.py")
        print("    ├── scripts/")
        print("    │   ├── procesador_json.py")
        print("    │   ├── consulta_db.py")
        print("    │   ├── dashboard.py")
        print("    │   ├── modelo_mejorado.py")
        print("    │   └── sistema_alertas.py")
        print("    ├── data/")
        print("    ├── config/")
        print("    └── ...")
        return
    
    # Ejecutar menu principal
    menu_principal()

if __name__ == "__main__":
    # Manejo de excepciones global
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSistema interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\nError inesperado en Main.py: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)