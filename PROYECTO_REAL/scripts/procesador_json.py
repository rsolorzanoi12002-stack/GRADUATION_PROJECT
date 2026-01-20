import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import shutil
from modelo_mejorado import ModeloCalidadAire

# Importar sistema de alertas
try:
    from sistema_alertas import SistemaAlertas, NivelAlerta, TipoAlerta
    SISTEMA_ALERTAS_DISPONIBLE = True
except ImportError:
    SISTEMA_ALERTAS_DISPONIBLE = False
    print("Advertencia: Sistema de alertas no disponible. Ejecute sin alertas.")

class ProcesadorCalidadAire:
    def __init__(self, config_path='../config/config.json'):
        """Inicializa el procesador de calidad del aire"""
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.config = self._cargar_configuracion(config_path)
        self.modelo_ml = ModeloCalidadAire()  # Usar el nuevo modelo mejorado
        self.modelo_cargado = False
        
        # Inicializar sistema de alertas si esta disponible
        if SISTEMA_ALERTAS_DISPONIBLE:
            self.sistema_alertas = SistemaAlertas()
            print("Sistema de alertas inicializado")
        else:
            self.sistema_alertas = None
            print("Sistema de alertas no disponible")
        
    def _cargar_configuracion(self, config_path):
        """Carga la configuracion desde archivo JSON"""
        config_abs_path = os.path.join(self.proyecto_root, config_path)
        if os.path.exists(config_abs_path):
            with open(config_abs_path, 'r') as f:
                config_data = json.load(f)
                
                # Convertir estructura a la esperada por el procesador
                return {
                    "database_path": config_data.get('paths', {}).get('database', 'data/database/calidad_aire.db'),
                    "model_path": "models/random_forest_model.pkl",
                    "raw_data_path": config_data.get('paths', {}).get('input_dir', 'data/raw_json'),
                    "processed_path": config_data.get('paths', {}).get('output_dir', 'data/processed'),
                    "archive_path": "data/archive",
                    "umbral_co2_alto": 800,
                    "umbral_co2_critico": 1200
                }
        else:
            # Configuracion por defecto
            return {
                "database_path": "data/database/calidad_aire.db",
                "model_path": "models/random_forest_model.pkl",
                "raw_data_path": "data/raw_json",
                "processed_path": "data/processed",
                "archive_path": "data/archive",
                "umbral_co2_alto": 800,
                "umbral_co2_critico": 1200
            }
    
    def conectar_db(self):
        """Conecta a la base de datos SQLite"""
        db_path = os.path.join(self.proyecto_root, self.config['database_path'])
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return sqlite3.connect(db_path)
    
    def crear_tablas(self):
        """Crea las tablas necesarias en la base de datos - USANDO ESTRUCTURA REAL"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            
            # PRIMERO: Verificar si las tablas existen y tienen la estructura correcta
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tablas_existentes = [row[0] for row in cursor.fetchall()]
            
            # Tabla sensor_requests (YA EXISTE)
            if 'sensor_requests' not in tablas_existentes:
                cursor.execute('''
                    CREATE TABLE sensor_requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        device_id TEXT,
                        request_data TEXT,
                        processed_at TEXT,
                        archived INTEGER DEFAULT 0
                    )
                ''')
                print("  [DB] Tabla sensor_requests creada")
            else:
                # Verificar columnas de sensor_requests
                cursor.execute("PRAGMA table_info(sensor_requests)")
                columnas = [col[1] for col in cursor.fetchall()]
                columnas_requeridas = ['id', 'timestamp', 'device_id', 'request_data', 'processed_at', 'archived']
                for col in columnas_requeridas:
                    if col not in columnas:
                        print(f"  [ADVERTENCIA] Columna {col} no encontrada en sensor_requests")
            
            # Tabla sensor_responses (YA EXISTE) - ¡VERIFICAR NOMBRES DE COLUMNAS!
            if 'sensor_responses' not in tablas_existentes:
                cursor.execute('''
                    CREATE TABLE sensor_responses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        request_id INTEGER,
                        calidad_aire_pred TEXT,
                        co2_nivel TEXT,
                        temperature REAL,
                        humedad REAL,
                        presion REAL,
                        importancia_variables TEXT,
                        prediccion_detalle TEXT,
                        created_at TEXT,
                        FOREIGN KEY (request_id) REFERENCES sensor_requests(id)
                    )
                ''')
                print("  [DB] Tabla sensor_responses creada")
            else:
                # Verificar columnas de sensor_responses
                cursor.execute("PRAGMA table_info(sensor_responses)")
                columnas = cursor.fetchall()
                nombres_columnas = [col[1] for col in columnas]
                print(f"  [DB] Columnas de sensor_responses: {nombres_columnas}")
                
                # Verificar si 'temperature' existe (puede estar como 'temperatura')
                if 'temperature' not in nombres_columnas and 'temperatura' in nombres_columnas:
                    print("  [INFO] La tabla tiene 'temperatura' en lugar de 'temperature'")
            
            # Tabla archivos_procesados (YA EXISTE)
            if 'archivos_procesados' not in tablas_existentes:
                cursor.execute('''
                    CREATE TABLE archivos_procesados (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nombre_archivo TEXT UNIQUE,
                        fecha_procesado TEXT,
                        procesado INTEGER DEFAULT 1,
                        request_id INTEGER,
                        FOREIGN KEY (request_id) REFERENCES sensor_requests(id)
                    )
                ''')
                print("  [DB] Tabla archivos_procesados creada")
            
            # Tabla alertas_sistema (YA EXISTE)
            if 'alertas_sistema' not in tablas_existentes:
                cursor.execute('''
                    CREATE TABLE alertas_sistema (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        nivel TEXT NOT NULL,
                        tipo TEXT NOT NULL,
                        ubicacion TEXT,
                        mensaje TEXT NOT NULL,
                        datos_adicionales TEXT,
                        procesada INTEGER DEFAULT 0,
                        fecha_procesada TEXT
                    )
                ''')
                print("  [DB] Tabla alertas_sistema creada")
            
            conn.commit()
    
    def verificar_estructura_tablas(self):
        """Verifica que las tablas tengan la estructura correcta"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            
            # Verificar sensor_responses específicamente
            cursor.execute("PRAGMA table_info(sensor_responses)")
            columnas = cursor.fetchall()
            nombres_columnas = [col[1] for col in columnas]
            
            print("\n[DB] Verificando estructura de tablas:")
            print(f"  sensor_responses columns: {nombres_columnas}")
            
            # Verificar si necesitamos renombrar columnas
            columnas_requeridas = ['id', 'request_id', 'calidad_aire_pred', 'co2_nivel', 
                                  'temperature', 'humedad', 'presion', 'importancia_variables', 
                                  'prediccion_detalle', 'created_at']
            
            # Verificar cada columna requerida
            for col in columnas_requeridas:
                if col not in nombres_columnas:
                    print(f"  [ADVERTENCIA] Columna '{col}' no encontrada en sensor_responses")
                    
                    # Si falta 'temperature' pero existe 'temperatura', renombrar
                    if col == 'temperature' and 'temperatura' in nombres_columnas:
                        try:
                            cursor.execute('ALTER TABLE sensor_responses RENAME COLUMN temperatura TO temperature')
                            print(f"  [DB] Columna renombrada: 'temperatura' -> 'temperature'")
                            conn.commit()
                        except:
                            print(f"  [ERROR] No se pudo renombrar columna")
            
            return True
    
    def archivo_ya_procesado(self, nombre_archivo):
        """Verifica si un archivo ya fue procesado usando archivos_procesados"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, fecha_procesado 
                FROM archivos_procesados 
                WHERE nombre_archivo = ? AND procesado = 1
            ''', (nombre_archivo,))
            resultado = cursor.fetchone()
            
            if resultado:
                print(f"  [INFO] Archivo {nombre_archivo} ya fue procesado el {resultado[1]}")
                return resultado[0]  # Devuelve el ID si existe
            
            return None  # No ha sido procesado
    
    def registrar_archivo_procesado(self, nombre_archivo, request_id):
        """Registra que un archivo ha sido procesado - ACTUALIZA 'procesado' y 'fecha_procesado'"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO archivos_procesados 
                (nombre_archivo, fecha_procesado, procesado, request_id)
                VALUES (?, ?, ?, ?)
            ''', (nombre_archivo, datetime.now().isoformat(), 1, request_id))
            conn.commit()
            print(f"  [DB] Archivo registrado como procesado en archivos_procesados")
    
    def actualizar_request_como_procesado(self, request_id):
        """Actualiza el request con fecha de procesamiento - ACTUALIZA 'processed_at'"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE sensor_requests 
                SET processed_at = ?, archived = 1
                WHERE id = ?
            ''', (datetime.now().isoformat(), request_id))
            conn.commit()
            print(f"  [DB] Request {request_id} actualizado con processed_at")
    
    def registrar_alerta_en_db(self, alerta_data):
        """Registra una alerta en la base de datos - VERSION CORREGIDA"""
        if not self.sistema_alertas:
            return None
        
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            
            # CORRECCION: Guardar con procesada=0 (consistente con sistema_alertas.py)
            cursor.execute('''
                INSERT INTO alertas_sistema 
                (timestamp, nivel, tipo, ubicacion, mensaje, datos_adicionales, procesada, fecha_procesada)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                alerta_data.get('timestamp', datetime.now().isoformat()),
                alerta_data.get('nivel', 'INFO'),
                alerta_data.get('tipo', 'GENERAL'),
                alerta_data.get('ubicacion', 'Desconocida'),
                alerta_data.get('mensaje', ''),
                json.dumps(alerta_data.get('datos_adicionales', {})),
                0,  # procesada = 0 (consistente con sistema_alertas.py)
                None  # fecha_procesada = NULL inicialmente
            ))
            
            alerta_id = cursor.lastrowid
            conn.commit()
            print(f"  [DB] Alerta registrada (ID: {alerta_id}) con procesada=0")
            
            # Ahora marcar como procesada
            self.marcar_alerta_como_procesada(alerta_id)
            
            return alerta_id
    
    def marcar_alerta_como_procesada(self, alerta_id):
        """Marca una alerta como procesada - ACTUALIZA 'procesada' y 'fecha_procesada'"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            
            # Obtener datos actuales para actualizar
            cursor.execute('SELECT datos_adicionales FROM alertas_sistema WHERE id = ?', (alerta_id,))
            resultado = cursor.fetchone()
            
            datos_adicionales = {}
            if resultado and resultado[0]:
                try:
                    datos_adicionales = json.loads(resultado[0])
                    # Agregar informacion de procesamiento
                    datos_adicionales['procesado_en'] = datetime.now().isoformat()
                    datos_adicionales['procesado_por'] = 'procesador_json'
                except:
                    datos_adicionales = {'procesado_en': datetime.now().isoformat()}
            
            # Actualizar alerta como procesada
            cursor.execute('''
                UPDATE alertas_sistema 
                SET procesada = 1, 
                    fecha_procesada = ?,
                    datos_adicionales = ?
                WHERE id = ?
            ''', (
                datetime.now().isoformat(),
                json.dumps(datos_adicionales, ensure_ascii=False),
                alerta_id
            ))
            
            conn.commit()
            print(f"  [DB] Alerta {alerta_id} marcada como procesada")
    
    def extraer_caracteristicas(self, json_data):
        """Extrae caracteristicas del JSON para el modelo"""
        sensor_data = json_data.get('sensor_data', {})
        readings = sensor_data.get('readings', {})
        
        # Extraer valores de los sensores
        scd30 = readings.get('scd30', {})
        bme280 = readings.get('bme280', {})
        mq135 = readings.get('mq135', {})
        
        # Crear caracteristicas para el modelo
        features = {
            'co2': scd30.get('co2', 0),
            'temperatura_scd': scd30.get('temperature', 0),
            'humedad_scd': scd30.get('humidity', 0),
            'temperatura_bme': bme280.get('temperature', 0),
            'humedad_bme': bme280.get('humidity', 0),
            'presion': bme280.get('pressure', 0),
            'mq135_analog': mq135.get('analog_value', 0),
            'mq135_digital': mq135.get('digital_value', 0)
        }
        
        # Extraer hora del dia del timestamp
        try:
            timestamp = sensor_data.get('metadata', {}).get('timestamp', '')
            if timestamp:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                features['hora_dia'] = dt.hour
                features['dia_semana'] = dt.weekday()
        except:
            features['hora_dia'] = 12
            features['dia_semana'] = 0
        
        return features
    
    def clasificar_calidad_aire(self, co2, valor_prediccion=None):
        """Clasifica la calidad del aire basado en niveles de CO2"""
        # PRIORIDAD 1: Usar clasificacion basada en CO2
        if co2 < 450:
            return "Excelente", "Muy bueno"
        elif co2 < 600:
            return "Buena", "Aceptable"
        elif co2 < 800:
            return "Moderada", "Ligeramente elevado"
        elif co2 < 1000:
            return "Deficiente", "Elevado"
        elif co2 < 1200:
            return "Muy deficiente", "Muy elevado"
        else:
            return "Peligrosa", "Critico"
    
    def analizar_con_modelo(self, features):
        """Analiza los datos con el modelo mejorado"""
        # Preparar caracteristicas para el modelo
        datos_modelo = {
            'co2': features['co2'],
            'temperatura': features['temperatura_scd'],
            'humedad': features['humedad_scd'],
            'presion': features['presion'],
            'hora_dia': features['hora_dia'],
            'dia_semana': features['dia_semana']
        }
        
        # Usar el modelo mejorado
        resultado = self.modelo_ml.predecir(datos_modelo)
        
        return resultado['valor_prediccion'], resultado['importancia_caracteristicas']
    
    def verificar_alertas(self, json_data, features, calidad_aire):
        """Verifica y genera alertas basadas en los datos"""
        if self.sistema_alertas is None:
            return []
        
        alertas_generadas = []
        
        # Obtener ubicacion correctamente del JSON
        metadata = json_data.get('sensor_data', {}).get('metadata', {})
        ubicacion = metadata.get('location', 'Ubicacion Desconocida')
        
        # 1. Verificar calidad del aire
        datos_verificacion = {
            'co2': features['co2'],
            'temperatura': features['temperatura_scd'],
            'humedad': features['humedad_scd']
        }
        
        # Pasar la ubicacion al sistema de alertas
        alertas_calidad = self.sistema_alertas.verificar_calidad_aire(datos_verificacion, ubicacion)
        alertas_generadas.extend(alertas_calidad)
        
        # 2. Verificar datos incompletos
        alertas_incompletos = self.sistema_alertas.verificar_datos_incompletos(json_data)
        alertas_generadas.extend(alertas_incompletos)
        
        # 3. Si la calidad es Peligrosa, generar alerta especifica
        if calidad_aire == "Peligrosa":
            alertas_peligrosa = self.sistema_alertas.verificar_calidad_peligrosa(
                calidad_aire, datos_verificacion, ubicacion
            )
            alertas_generadas.extend(alertas_peligrosa)
        
        # 4. Registrar cada alerta en la base de datos
        for alerta in alertas_generadas:
            alerta_id = self.registrar_alerta_en_db(alerta)
            if alerta_id:
                alerta['db_id'] = alerta_id  # Guardar ID para referencia
        
        return alertas_generadas
    
    def guardar_request(self, json_data, device_id, timestamp):
        """Guarda el request (JSON original) en la base de datos con processed_at = NULL inicialmente"""
        with self.conectar_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO sensor_requests 
                (timestamp, device_id, request_data, processed_at, archived)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, device_id, json.dumps(json_data), 
                  None,  # processed_at = NULL inicialmente
                  0))    # archived = 0 significa no archivado
            request_id = cursor.lastrowid
            conn.commit()
            print(f"  [DB] Request guardado (ID: {request_id}) con processed_at=NULL")
        return request_id
    
    def guardar_response(self, request_id, response_data):
        """Guarda el response (analisis) en la base de datos - CORREGIDO con manejo de errores"""
        try:
            with self.conectar_db() as conn:
                cursor = conn.cursor()
                
                # Verificar estructura de la tabla antes de insertar
                cursor.execute("PRAGMA table_info(sensor_responses)")
                columnas = cursor.fetchall()
                nombres_columnas = [col[1] for col in columnas]
                
                # IMPORTANTE: La columna se llama 'temperature' en la BD, no 'temperatura'
                # Mapear nombres de variables a nombres de columnas
                datos_para_insertar = {
                    'request_id': request_id,
                    'calidad_aire_pred': response_data['calidad_aire'],
                    'co2_nivel': response_data['co2_nivel'],
                    'temperature': response_data['temperatura'],
                    'humedad': response_data['humedad'],
                    'presion': response_data['presion'],
                    'importancia_variables': json.dumps(response_data['importancia_variables']),
                    'prediccion_detalle': json.dumps({
                        'prediccion_valor': response_data['prediccion_valor'],
                        'co2_ppm': response_data['co2_ppm'],
                        'recomendaciones': response_data['recomendaciones'],
                        'ubicacion': response_data.get('ubicacion', 'Desconocida'),
                        'features_utilizadas': response_data['features_utilizadas'],
                        'info_alertas': response_data['info_alertas']
                    }),
                    'created_at': response_data['timestamp_analisis']
                }
                
                # Construir query dinamica basada en columnas disponibles
                columnas_disponibles = []
                valores = []
                
                for col, val in datos_para_insertar.items():
                    if col in nombres_columnas:
                        columnas_disponibles.append(col)
                        valores.append(val)
                    else:
                        print(f"  [ADVERTENCIA] Columna '{col}' no encontrada en sensor_responses, omitiendo")
                
                if not columnas_disponibles:
                    raise ValueError("No hay columnas validas para insertar en sensor_responses")
                
                # Crear query dinamica
                placeholders = ','.join(['?'] * len(columnas_disponibles))
                columnas_sql = ','.join(columnas_disponibles)
                
                query = f'''
                    INSERT INTO sensor_responses 
                    ({columnas_sql})
                    VALUES ({placeholders})
                '''
                
                cursor.execute(query, valores)
                conn.commit()
                print(f"  [DB] Response guardado para request {request_id}")
                print(f"  [DB] Columnas insertadas: {columnas_disponibles}")
                
                return True  # Retornar exito
                
        except Exception as e:
            print(f"  [ERROR DB] Error guardando response: {e}")
            # Mostrar informacion de depuracion
            print(f"  [DEBUG] Columnas disponibles en BD: {nombres_columnas}")
            print(f"  [DEBUG] Datos a insertar keys: {list(datos_para_insertar.keys())}")
            # Registrar en log
            self._registrar_error_en_log(f"Error en guardar_response para request {request_id}: {e}")
            return False  # Retornar fallo
    
    def _registrar_error_en_log(self, mensaje):
        """Registra error en archivo log"""
        log_path = os.path.join(self.proyecto_root, 'logs', 'errores_procesamiento.log')
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        with open(log_path, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().isoformat()
            f.write(f"[{timestamp}] {mensaje}\n")
    
    def procesar_json(self, json_path):
        """Procesa un archivo JSON individual - Optimizado para procesamiento uno por uno"""
        nombre_archivo = os.path.basename(json_path)
        print(f"\nProcesando: {nombre_archivo}")
        
        # Verificar si el archivo ya fue procesado
        archivo_id = self.archivo_ya_procesado(nombre_archivo)
        if archivo_id:
            print(f"  [SALTADO] Archivo ya procesado anteriormente")
            return None  # Saltar este archivo
        
        # Leer JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Extraer metadata
        metadata = json_data.get('sensor_data', {}).get('metadata', {})
        device_id = metadata.get('device_id', 'DESCONOCIDO')
        timestamp = metadata.get('timestamp', '')
        ubicacion = metadata.get('location', 'Ubicacion Desconocida')
        
        # 1. Guardar request en BD (con processed_at = NULL)
        request_id = self.guardar_request(json_data, device_id, timestamp)
        
        try:
            # Extraer caracteristicas
            features = self.extraer_caracteristicas(json_data)
            
            # Analizar con modelo (para obtener importancia de variables)
            prediccion, importancias = self.analizar_con_modelo(features)
            
            # Clasificar calidad del aire usando SOLO CO2
            calidad_aire, co2_nivel = self.clasificar_calidad_aire(features['co2'])
            
            # Verificar y generar alertas (con ubicacion correcta)
            alertas_generadas = self.verificar_alertas(json_data, features, calidad_aire)
            
            # Crear response con informacion de alertas
            info_alertas = {
                'total_alertas': len(alertas_generadas),
                'alertas_criticas': sum(1 for a in alertas_generadas if a.get('nivel') == 'CRITICA'),
                'alertas_generadas': alertas_generadas
            }
            
            # Crear response - Asegurar que las claves coincidan con nombres de columnas
            response_data = {
                'calidad_aire': calidad_aire,
                'co2_nivel': co2_nivel,
                'co2_ppm': features['co2'],
                'temperatura': features['temperatura_scd'],
                'humedad': features['humedad_scd'],
                'presion': features['presion'],
                'prediccion_valor': float(prediccion),
                'importancia_variables': importancias,
                'timestamp_analisis': datetime.now().isoformat(),
                'ubicacion': ubicacion,
                'recomendaciones': self.generar_recomendaciones(calidad_aire, features['co2']),
                'features_utilizadas': list(importancias.keys()),
                'info_alertas': info_alertas
            }
            
            # 2. Guardar response en BD - VERIFICAR RETORNO
            if not self.guardar_response(request_id, response_data):
                raise Exception("Error al guardar response en base de datos")
            
            # 3. Actualizar request como procesado (processed_at = fecha actual)
            self.actualizar_request_como_procesado(request_id)
            
            # 4. Registrar que el archivo fue procesado en archivos_procesados
            self.registrar_archivo_procesado(nombre_archivo, request_id)
            
            # 5. Mover archivo a archive
            self.archivar_json(json_path)
            
            # Mostrar resumen
            print(f"  -> Ubicacion: {ubicacion}")
            print(f"  -> Calidad del aire: {calidad_aire} (CO2: {features['co2']} ppm)")
            print(f"  -> Temperatura: {features['temperatura_scd']}°C")
            
            if alertas_generadas:
                print(f"  -> Alertas generadas: {len(alertas_generadas)}")
            
            return response_data
            
        except Exception as e:
            # Si hay error en el procesamiento, NO actualizar request como procesado
            print(f"  [ERROR] Error procesando JSON: {e}")
            
            # Registrar error en log
            self._registrar_error_en_log(f"Error procesando {nombre_archivo}: {e}")
            
            raise  # Re-lanzar excepcion para manejo superior
    
    def archivar_json(self, json_path):
        """Mueve el JSON procesado a la carpeta de archivo"""
        nombre_archivo = os.path.basename(json_path)
        destino = os.path.join(self.proyecto_root, self.config['archive_path'], nombre_archivo)
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        
        # Mover archivo a archive
        try:
            shutil.move(json_path, destino)
            print(f"  -> Archivo movido a: {destino}")
        except Exception as e:
            print(f"  -> Error moviendo archivo: {e}")
            # Si falla el move, hacer copy y delete
            try:
                shutil.copy2(json_path, destino)
                os.remove(json_path)
                print(f"  -> Archivo copiado y eliminado original")
            except Exception as e2:
                print(f"  -> Error critico: {e2}")
    
    def generar_recomendaciones(self, calidad_aire, co2):
        """Genera recomendaciones basadas en la calidad del aire"""
        if calidad_aire == "Excelente":
            return "Condiciones optimas. Mantener ventilacion normal."
        elif calidad_aire == "Buena":
            return "Condiciones aceptables. Verificar fuentes de emision."
        elif calidad_aire == "Moderada":
            return "Aumentar ventilacion. Considerar reducir actividades intensas."
        elif calidad_aire == "Deficiente":
            return "Ventilacion forzada recomendada. Monitorear continuamente."
        elif calidad_aire == "Muy deficiente":
            return "ALERTA: Condiciones peligrosas. Limitar actividades."
        else:  # Peligrosa
            return "ALERTA CRITICA: Evitar exposicion. Activar sistemas de emergencia."
    
    def procesar_uno_por_uno(self):
        """Procesa todos los archivos JSON uno por uno para mayor estabilidad"""
        raw_dir = os.path.join(self.proyecto_root, self.config['raw_data_path'])
        
        if not os.path.exists(raw_dir):
            print(f"Error: Directorio {raw_dir} no existe")
            return
        
        # Crear tablas si no existen y verificar estructura
        self.crear_tablas()
        self.verificar_estructura_tablas()
        
        # Listar archivos JSON
        archivos_json = [f for f in os.listdir(raw_dir) if f.endswith('.json')]
        
        if not archivos_json:
            print("No se encontraron archivos JSON para procesar")
            return
        
        print(f"Encontrados {len(archivos_json)} archivos JSON para procesar")
        print("Procesando UNO POR UNO para mayor estabilidad...")
        print("-" * 50)
        
        resultados = []
        total_alertas = 0
        procesados_exitosamente = 0
        procesados_con_error = 0
        ya_procesados = 0
        
        for i, archivo in enumerate(archivos_json, 1):
            json_path = os.path.join(raw_dir, archivo)
            print(f"\n[{i}/{len(archivos_json)}] {archivo}")
            
            try:
                resultado = self.procesar_json(json_path)
                
                if resultado:
                    resultados.append(resultado)
                    procesados_exitosamente += 1
                    
                    if 'info_alertas' in resultado:
                        total_alertas += resultado['info_alertas']['total_alertas']
                else:
                    ya_procesados += 1
                    
            except Exception as e:
                print(f"   Error procesando: {e}")
                procesados_con_error += 1
                
                import time
                time.sleep(1)  # Pequena pausa despues de un error
        
        print("-" * 50)
        print("RESUMEN DEL PROCESAMIENTO:")
        print(f"   Exitosos: {procesados_exitosamente}")
        print(f"   Errores: {procesados_con_error}")
        print(f"   Ya procesados: {ya_procesados}")
        print(f"   Total archivos: {len(archivos_json)}")
        
        if total_alertas > 0:
            print(f"   Alertas generadas: {total_alertas}")
        
        # Verificar alertas pendientes
        if self.sistema_alertas:
            self.sistema_alertas.verificar_alertas_pendientes()
        
        # Generar reporte resumen
        if resultados:
            self.generar_reporte(resultados, total_alertas)
        
        return resultados
    
    def generar_reporte(self, resultados, total_alertas=0):
        """Genera un reporte resumen del procesamiento"""
        if not resultados:
            return
        
        reporte_path = os.path.join(self.proyecto_root, 'reports', 
                                   f'reporte_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        os.makedirs(os.path.dirname(reporte_path), exist_ok=True)
        
        # Calcular estadisticas avanzadas
        co2_values = [r['co2_ppm'] for r in resultados]
        predicciones = [r['prediccion_valor'] for r in resultados]
        
        # Contar alertas por nivel
        alertas_criticas = 0
        alertas_advertencia = 0
        for resultado in resultados:
            if 'info_alertas' in resultado:
                alertas_criticas += resultado['info_alertas']['alertas_criticas']
                alertas_advertencia += resultado['info_alertas']['total_alertas'] - resultado['info_alertas']['alertas_criticas']
        
        # Agrupar por ubicacion
        ubicaciones = {}
        for resultado in resultados:
            ubicacion = resultado.get('ubicacion', 'Desconocida')
            if ubicacion not in ubicaciones:
                ubicaciones[ubicacion] = []
            ubicaciones[ubicacion].append(resultado)
        
        reporte = {
            'fecha_generacion': datetime.now().isoformat(),
            'total_procesados': len(resultados),
            'resumen_calidad_aire': {},
            'resumen_por_ubicacion': {},
            'resumen_alertas': {
                'total_alertas': total_alertas,
                'alertas_criticas': alertas_criticas,
                'alertas_advertencia': alertas_advertencia
            },
            'estadisticas_basicas': {
                'co2_promedio': np.mean(co2_values),
                'co2_min': np.min(co2_values),
                'co2_max': np.max(co2_values),
                'temperatura_promedio': np.mean([r['temperatura'] for r in resultados]),
                'humedad_promedio': np.mean([r['humedad'] for r in resultados])
            },
            'estadisticas_modelo': {
                'prediccion_promedio': np.mean(predicciones),
                'prediccion_min': np.min(predicciones),
                'prediccion_max': np.max(predicciones)
            },
            'resultados_detallados': resultados[:10]
        }
        
        # Contar frecuencia de cada categoria
        categorias = [r['calidad_aire'] for r in resultados]
        for cat in set(categorias):
            reporte['resumen_calidad_aire'][cat] = categorias.count(cat)
        
        # Resumen por ubicacion
        for ubicacion, datos in ubicaciones.items():
            co2_ubicacion = [d['co2_ppm'] for d in datos]
            categorias_ubicacion = [d['calidad_aire'] for d in datos]
            
            reporte['resumen_por_ubicacion'][ubicacion] = {
                'total_muestras': len(datos),
                'co2_promedio': np.mean(co2_ubicacion),
                'co2_min': np.min(co2_ubicacion),
                'co2_max': np.max(co2_ubicacion),
                'categorias': {cat: categorias_ubicacion.count(cat) for cat in set(categorias_ubicacion)}
            }
        
        # Calcular importancia promedio de variables
        if resultados and 'importancia_variables' in resultados[0]:
            importancias_todas = [r['importancia_variables'] for r in resultados]
            importancias_promedio = {}
            for var in importancias_todas[0].keys():
                valores = [imp[var] for imp in importancias_todas if var in imp]
                if valores:
                    importancias_promedio[var] = np.mean(valores)
            reporte['importancia_variables_promedio'] = importancias_promedio
        
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)
        
        print(f"\nReporte generado: {reporte_path}")
        
        # Mostrar resumen en consola
        print("\nRESUMEN DEL PROCESAMIENTO:")
        print(f"  Total muestras: {len(resultados)}")
        print(f"  CO2 promedio: {np.mean(co2_values):.1f} ppm")
        print(f"  Rango CO2: {np.min(co2_values):.1f} - {np.max(co2_values):.1f} ppm")
        
        for cat, count in reporte['resumen_calidad_aire'].items():
            porcentaje = (count / len(resultados)) * 100
            print(f"  {cat}: {count} muestras ({porcentaje:.1f}%)")
        
        if total_alertas > 0:
            print(f"\n  Alertas generadas: {total_alertas}")
            print(f"    Criticas: {alertas_criticas}")
            print(f"    Advertencias: {alertas_advertencia}")
        
        # Mostrar resumen por ubicacion
        if len(reporte['resumen_por_ubicacion']) > 0:
            print(f"\n  Resumen por ubicacion:")
            for ubicacion, datos in reporte['resumen_por_ubicacion'].items():
                print(f"    {ubicacion}: {datos['total_muestras']} muestras, CO2: {datos['co2_promedio']:.1f} ppm")

def main():
    """Funcion principal"""
    print("PROCESADOR DE CALIDAD DEL AIRE - UPS GUAYAQUIL")
    print("Version corregida - Procesamiento uno por uno")
    print("=" * 50)
    
    # Mostrar estado del sistema de alertas
    if SISTEMA_ALERTAS_DISPONIBLE:
        print("Sistema de alertas: ACTIVADO")
    else:
        print("Sistema de alertas: NO DISPONIBLE (ejecutar sin alertas)")
    
    print("=" * 50)
    
    procesador = ProcesadorCalidadAire()
    
    # Procesar todos los JSON UNO POR UNO
    resultados = procesador.procesar_uno_por_uno()
    
    print("=" * 50)
    print("Proceso finalizado exitosamente")
    
    # Mostrar estado final de las variables
    print("\nESTADO FINAL DE LAS VARIABLES DE PROCESAMIENTO:")
    print("  - sensor_requests.processed_at: Actualizado con fecha de procesamiento")
    print("  - archivos_procesados.procesado: Actualizado a 1 (procesado)")
    print("  - archivos_procesados.fecha_procesado: Actualizado con fecha")
    print("  - alertas_sistema.procesada: Actualizado a 1 (procesada)")
    print("  - alertas_sistema.fecha_procesada: Actualizado con fecha")

if __name__ == "__main__":
    main()