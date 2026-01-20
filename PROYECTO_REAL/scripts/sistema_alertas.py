import os
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
import sqlite3

class NivelAlerta(Enum):
    """Niveles de alerta"""
    INFO = "INFO"
    ADVERTENCIA = "ADVERTENCIA"
    CRITICA = "CRITICA"

class TipoAlerta(Enum):
    """Tipos de alerta"""
    CALIDAD_AIRE = "CALIDAD_AIRE"
    SENSOR_FALLIDO = "SENSOR_FALLIDO"
    DATOS_INCOMPLETOS = "DATOS_INCOMPLETOS"
    MODELO_ERROR = "MODELO_ERROR"
    SISTEMA = "SISTEMA"

class SistemaAlertas:
    def __init__(self):
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.logs_dir = os.path.join(self.proyecto_root, 'logs')
        self.alertas_dir = os.path.join(self.proyecto_root, 'data', 'alertas')
        
        # Crear directorios si no existen
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(self.alertas_dir, exist_ok=True)
        
        # Configurar logging
        self.configurar_logging()
        
        # Actualizar tabla de alertas si es necesario
        self.actualizar_tabla_alertas()
        
        # Umbrales para alertas
        self.umbrales = {
            'co2_critico': 1200,      # ppm
            'co2_alto': 1000,         # ppm
            'temperatura_alta': 35,   # °C
            'temperatura_baja': 15,   # °C
            'humedad_alta': 85,       # %
            'humedad_baja': 30        # %
        }
        
        # Cache para evitar alertas duplicadas en corto tiempo
        self.ultimas_alertas = {}  # formato: {clave_alerta: timestamp}
        self.tiempo_minimo_entre_alertas = {
            'CALIDAD_AIRE': 300,      # 5 minutos para alertas de calidad
            'SENSOR_FALLIDO': 600,    # 10 minutos para fallos de sensor
            'DATOS_INCOMPLETOS': 300, # 5 minutos
            'MODELO_ERROR': 300,      # 5 minutos
            'SISTEMA': 60             # 1 minuto
        }
    
    def actualizar_tabla_alertas(self):
        """Actualiza la tabla de alertas con estructura completa"""
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            print(f"Base de datos no encontrada en {db_path}")
            return False
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 1. Eliminar tabla antigua corrupta si existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alertas'")
            if cursor.fetchone():
                cursor.execute("DROP TABLE IF EXISTS alertas")
                self.logger.info("Tabla 'alertas' antigua eliminada")
            
            # 2. Crear tabla alertas_sistema si no existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alertas_sistema'")
            if not cursor.fetchone():
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
                self.logger.info("Tabla 'alertas_sistema' creada")
            
            # 3. Verificar y agregar columnas faltantes
            cursor.execute("PRAGMA table_info(alertas_sistema)")
            columnas_existentes = {col[1] for col in cursor.fetchall()}
            
            # Lista completa de columnas requeridas
            columnas_requeridas = [
                ('timestamp', 'TEXT NOT NULL'),
                ('nivel', 'TEXT NOT NULL'),
                ('tipo', 'TEXT NOT NULL'),
                ('ubicacion', 'TEXT'),
                ('mensaje', 'TEXT NOT NULL'),
                ('datos_adicionales', 'TEXT'),
                ('procesada', 'INTEGER DEFAULT 0'),
                ('fecha_procesada', 'TEXT')
            ]
            
            for columna, tipo in columnas_requeridas:
                if columna not in columnas_existentes:
                    cursor.execute(f'ALTER TABLE alertas_sistema ADD COLUMN {columna} {tipo}')
                    self.logger.info(f"Columna '{columna}' agregada")
            
            # 4. Crear índices si no existen
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_alertas_timestamp'")
            if not cursor.fetchone():
                cursor.execute('CREATE INDEX idx_alertas_timestamp ON alertas_sistema(timestamp)')
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_alertas_procesada'")
            if not cursor.fetchone():
                cursor.execute('CREATE INDEX idx_alertas_procesada ON alertas_sistema(procesada)')
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_alertas_nivel'")
            if not cursor.fetchone():
                cursor.execute('CREATE INDEX idx_alertas_nivel ON alertas_sistema(nivel)')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Estructura de tabla alertas_sistema verificada y actualizada")
            return True
            
        except Exception as e:
            self.logger.error(f"Error actualizando tabla de alertas: {e}")
            return False
    
    def configurar_logging(self):
        """Configura el sistema de logging"""
        log_file = os.path.join(self.logs_dir, 'alertas.log')
        
        # Configurar formato
        formato = '%(asctime)s - %(levelname)s - %(message)s'
        formato_fecha = '%Y-%m-%d %H:%M:%S'
        
        # Configurar logger
        self.logger = logging.getLogger('SistemaAlertas')
        self.logger.setLevel(logging.DEBUG)
        
        # Evitar duplicacion de handlers
        if not self.logger.handlers:
            # Handler para archivo
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(formato, formato_fecha)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            # Handler para consola (solo warnings y criticos)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.WARNING)
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
    
    def deberia_generar_alerta(self, tipo, datos_clave=None, ubicacion="general"):
        """Verifica si se debe generar una alerta (evita duplicados)"""
        ahora = datetime.now()
        
        # Crear clave unica para esta alerta
        if datos_clave:
            clave = f"{tipo.value}_{ubicacion}_{datos_clave}"
        else:
            clave = f"{tipo.value}_{ubicacion}"
        
        # Verificar si ya hubo una alerta similar recientemente
        if clave in self.ultimas_alertas:
            tiempo_desde_ultima = (ahora - self.ultimas_alertas[clave]).total_seconds()
            tiempo_minimo = self.tiempo_minimo_entre_alertas.get(tipo.value, 300)
            
            if tiempo_desde_ultima < tiempo_minimo:
                # No generar alerta, aun esta en el periodo de espera
                self.logger.debug(f"Alerta suprimida (duplicada reciente): {clave}")
                return False
        
        # Actualizar timestamp
        self.ultimas_alertas[clave] = ahora
        
        # Limpiar cache viejo (mas de 1 hora)
        claves_a_eliminar = []
        for key, timestamp in self.ultimas_alertas.items():
            if (ahora - timestamp).total_seconds() > 3600:
                claves_a_eliminar.append(key)
        
        for key in claves_a_eliminar:
            del self.ultimas_alertas[key]
        
        return True
    
    def registrar_alerta(self, nivel, tipo, mensaje, datos_adicionales=None, ubicacion="general"):
        """Registra una alerta en el sistema"""
        timestamp = datetime.now().isoformat()
        
        # Crear objeto de alerta
        alerta = {
            'timestamp': timestamp,
            'nivel': nivel.value,
            'tipo': tipo.value,
            'mensaje': mensaje,
            'ubicacion': ubicacion,
            'datos_adicionales': datos_adicionales or {}
        }
        
        # Registrar en log
        if nivel == NivelAlerta.CRITICA:
            self.logger.critical(f"{tipo.value} [{ubicacion}]: {mensaje}")
            self.mostrar_alerta_consola(alerta, color='red')
        elif nivel == NivelAlerta.ADVERTENCIA:
            self.logger.warning(f"{tipo.value} [{ubicacion}]: {mensaje}")
            self.mostrar_alerta_consola(alerta, color='yellow')
        else:
            self.logger.info(f"{tipo.value} [{ubicacion}]: {mensaje}")
        
        # Guardar en archivo JSON
        self.guardar_alerta_json(alerta)
        
        # Guardar en base de datos
        self.guardar_alerta_db(alerta)
        
        return alerta
    
    def mostrar_alerta_consola(self, alerta, color='white'):
        """Muestra alerta en consola con colores"""
        colores = {
            'red': '\033[91m',
            'yellow': '\033[93m',
            'green': '\033[92m',
            'blue': '\033[94m',
            'reset': '\033[0m'
        }
        
        color_code = colores.get(color, colores['reset'])
        ubicacion = alerta.get('ubicacion', 'Desconocida')
        
        print(f"\n{color_code}{'='*60}")
        print(f"ALERTA: {alerta['nivel']} - {alerta['tipo']}")
        print(f"Ubicacion: {ubicacion}")
        print(f"Hora: {alerta['timestamp']}")
        print(f"Mensaje: {alerta['mensaje']}")
        
        if alerta['datos_adicionales']:
            print("Datos adicionales:")
            for key, value in alerta['datos_adicionales'].items():
                print(f"  {key}: {value}")
        
        print(f"{'='*60}{colores['reset']}\n")
    
    def guardar_alerta_json(self, alerta):
        """Guarda la alerta en un archivo JSON"""
        fecha = datetime.now().strftime("%Y%m%d")
        archivo_alertas = os.path.join(self.alertas_dir, f'alertas_{fecha}.json')
        
        # Cargar alertas existentes o crear lista nueva
        if os.path.exists(archivo_alertas):
            with open(archivo_alertas, 'r', encoding='utf-8') as f:
                try:
                    alertas_existentes = json.load(f)
                except:
                    alertas_existentes = []
        else:
            alertas_existentes = []
        
        # Agregar nueva alerta
        alertas_existentes.append(alerta)
        
        # Guardar
        with open(archivo_alertas, 'w', encoding='utf-8') as f:
            json.dump(alertas_existentes, f, indent=2, ensure_ascii=False)
    
    def guardar_alerta_db(self, alerta):
        """Guarda la alerta en la base de datos - VERSION CORREGIDA"""
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            self.logger.error(f"Base de datos no encontrada: {db_path}")
            return
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # CORRECCION: Guardar con procesada=0 y fecha_procesada=NULL
            # El procesador se encargara de marcarla como procesada
            cursor.execute('''
                INSERT INTO alertas_sistema 
                (timestamp, nivel, tipo, ubicacion, mensaje, datos_adicionales, procesada, fecha_procesada)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                alerta['timestamp'],
                alerta['nivel'],
                alerta['tipo'],
                alerta.get('ubicacion', 'general'),
                alerta['mensaje'],
                json.dumps(alerta['datos_adicionales']),
                0,      # procesada = 0 (no procesada aun)
                None    # fecha_procesada = NULL (se actualizara cuando se procese)
            ))
            
            alerta_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            self.logger.info(f"Alerta guardada en BD (ID: {alerta_id}): {alerta['mensaje'][:50]}...")
            
        except Exception as e:
            self.logger.error(f"Error guardando alerta en BD: {e}")
    
    def verificar_calidad_aire(self, datos_sensor, ubicacion="Desconocida"):
        """Verifica la calidad del aire con deduplicacion"""
        alertas_generadas = []
        
        # Verificar CO2 critico
        co2 = datos_sensor.get('co2', 0)
        if co2 >= self.umbrales['co2_critico']:
            clave = "co2_critico"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.CRITICA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Nivel de CO2 CRITICO: {co2} ppm",
                    datos_adicionales={
                        'co2_ppm': co2,
                        'umbral': self.umbrales['co2_critico'],
                        'condicion': 'CO2_CRITICO',
                        'recomendacion': 'EVACUAR AREA - Ventilacion de emergencia requerida'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        # Verificar CO2 alto
        elif co2 >= self.umbrales['co2_alto']:
            clave = "co2_alto"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Nivel de CO2 ALTO: {co2} ppm",
                    datos_adicionales={
                        'co2_ppm': co2,
                        'umbral': self.umbrales['co2_alto'],
                        'condicion': 'CO2_ALTO',
                        'recomendacion': 'Aumentar ventilacion - Monitorear continuamente'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        # Verificar temperatura alta
        temperatura = datos_sensor.get('temperatura', 0)
        if temperatura >= self.umbrales['temperatura_alta']:
            clave = "temp_alta"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Temperatura ALTA: {temperatura}°C",
                    datos_adicionales={
                        'temperatura_c': temperatura,
                        'umbral': self.umbrales['temperatura_alta'],
                        'condicion': 'TEMPERATURA_ALTA',
                        'recomendacion': 'Activar sistemas de enfriamiento'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        # Verificar temperatura baja
        elif temperatura <= self.umbrales['temperatura_baja']:
            clave = "temp_baja"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Temperatura BAJA: {temperatura}°C",
                    datos_adicionales={
                        'temperatura_c': temperatura,
                        'umbral': self.umbrales['temperatura_baja'],
                        'condicion': 'TEMPERATURA_BAJA',
                        'recomendacion': 'Activar sistemas de calefaccion'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        # Verificar humedad alta
        humedad = datos_sensor.get('humedad', 0)
        if humedad >= self.umbrales['humedad_alta']:
            clave = "hum_alta"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Humedad ALTA: {humedad}%",
                    datos_adicionales={
                        'humedad_percent': humedad,
                        'umbral': self.umbrales['humedad_alta'],
                        'condicion': 'HUMEDAD_ALTA',
                        'recomendacion': 'Controlar humedad - Riesgo de condensacion'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        # Verificar humedad baja
        elif humedad <= self.umbrales['humedad_baja']:
            clave = "hum_baja"
            if self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.CALIDAD_AIRE,
                    mensaje=f"Humedad BAJA: {humedad}%",
                    datos_adicionales={
                        'humedad_percent': humedad,
                        'umbral': self.umbrales['humedad_baja'],
                        'condicion': 'HUMEDAD_BAJA',
                        'recomendacion': 'Aumentar humedad - Riesgo de sequedad'
                    },
                    ubicacion=ubicacion
                )
                alertas_generadas.append(alerta)
        
        return alertas_generadas
    
    def verificar_calidad_peligrosa(self, calidad_aire, datos_sensor, ubicacion="Desconocida"):
        """Verifica alerta especifica para calidad 'Peligrosa'"""
        if calidad_aire != "Peligrosa":
            return []
        
        clave = "calidad_peligrosa"
        if not self.deberia_generar_alerta(TipoAlerta.CALIDAD_AIRE, clave, ubicacion):
            return []
        
        alerta = self.registrar_alerta(
            nivel=NivelAlerta.CRITICA,
            tipo=TipoAlerta.CALIDAD_AIRE,
            mensaje=f"Calidad del aire PELIGROSA detectada",
            datos_adicionales={
                'co2_ppm': datos_sensor.get('co2', 0),
                'temperatura': datos_sensor.get('temperatura', 0),
                'humedad': datos_sensor.get('humedad', 0),
                'calidad_detectada': calidad_aire,
                'condicion': 'CALIDAD_PELIGROSA',
                'recomendacion': 'EVACUACION RECOMENDADA - Activar sistemas de emergencia'
            },
            ubicacion=ubicacion
        )
        
        return [alerta]
    
    def verificar_datos_incompletos(self, json_data):
        """Verifica si los datos del sensor estan incompletos"""
        alertas = []
        
        sensor_data = json_data.get('sensor_data', {})
        readings = sensor_data.get('readings', {})
        metadata = sensor_data.get('metadata', {})
        ubicacion = metadata.get('location', 'Desconocida')
        
        # Verificar sensores individuales
        sensores_faltantes = []
        
        if 'scd30' not in readings:
            sensores_faltantes.append('SCD30 (CO2)')
        else:
            scd30 = readings['scd30']
            if not all(key in scd30 for key in ['co2', 'temperature', 'humidity']):
                sensores_faltantes.append('SCD30 datos incompletos')
        
        if 'bme280' not in readings:
            sensores_faltantes.append('BME280 (meteorologico)')
        else:
            bme280 = readings['bme280']
            if not all(key in bme280 for key in ['temperature', 'humidity', 'pressure']):
                sensores_faltantes.append('BME280 datos incompletos')
        
        if 'mq135' not in readings:
            sensores_faltantes.append('MQ135 (calidad aire)')
        
        if sensores_faltantes:
            clave = f"datos_incompletos_{hash(str(sensores_faltantes))}"
            if self.deberia_generar_alerta(TipoAlerta.DATOS_INCOMPLETOS, clave, ubicacion):
                alerta = self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.DATOS_INCOMPLETOS,
                    mensaje=f"Datos de sensores incompletos: {', '.join(sensores_faltantes)}",
                    datos_adicionales={
                        'sensores_faltantes': sensores_faltantes,
                        'timestamp': metadata.get('timestamp', ''),
                        'recomendacion': 'Verificar conexion de sensores'
                    },
                    ubicacion=ubicacion
                )
                alertas.append(alerta)
        
        return alertas
    
    def verificar_alertas_pendientes(self):
        """Verifica si hay alertas no procesadas en la BD"""
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            return {}
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Verificar alertas criticas pendientes (mas de 1 hora)
            cursor.execute('''
                SELECT COUNT(*) FROM alertas_sistema 
                WHERE procesada = 0 AND nivel = 'CRITICA'
                AND datetime(timestamp) <= datetime('now', '-1 hours')
            ''')
            alertas_criticas_pendientes = cursor.fetchone()[0]
            
            # Verificar alertas de advertencia pendientes (mas de 4 horas)
            cursor.execute('''
                SELECT COUNT(*) FROM alertas_sistema 
                WHERE procesada = 0 AND nivel = 'ADVERTENCIA'
                AND datetime(timestamp) <= datetime('now', '-4 hours')
            ''')
            alertas_advertencia_pendientes = cursor.fetchone()[0]
            
            conn.close()
            
            # Generar alerta del sistema si hay alertas viejas pendientes
            if alertas_criticas_pendientes > 0:
                self.registrar_alerta(
                    nivel=NivelAlerta.CRITICA,
                    tipo=TipoAlerta.SISTEMA,
                    mensaje=f"Existen {alertas_criticas_pendientes} alertas CRITICAS pendientes por mas de 1 hora",
                    datos_adicionales={
                        'alertas_criticas_pendientes': alertas_criticas_pendientes,
                        'tiempo_pendiente': '>1 hora',
                        'recomendacion': 'Revisar inmediatamente las alertas del sistema'
                    },
                    ubicacion='Sistema'
                )
            
            elif alertas_advertencia_pendientes > 0:
                self.registrar_alerta(
                    nivel=NivelAlerta.ADVERTENCIA,
                    tipo=TipoAlerta.SISTEMA,
                    mensaje=f"Existen {alertas_advertencia_pendientes} alertas de ADVERTENCIA pendientes por mas de 4 horas",
                    datos_adicionales={
                        'alertas_advertencia_pendientes': alertas_advertencia_pendientes,
                        'tiempo_pendiente': '>4 horas',
                        'recomendacion': 'Revisar las alertas pendientes'
                    },
                    ubicacion='Sistema'
                )
            
            return {
                'criticas_pendientes': alertas_criticas_pendientes,
                'advertencias_pendientes': alertas_advertencia_pendientes
            }
            
        except Exception as e:
            self.logger.error(f"Error verificando alertas pendientes: {e}")
            return {}
    
    def obtener_alertas_pendientes(self, nivel=None, ubicacion=None, limite=20):
        """Obtiene alertas pendientes de la base de datos"""
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            return []
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Construir query basada en filtros
            query = '''
                SELECT id, timestamp, nivel, tipo, ubicacion, mensaje, datos_adicionales, 
                       procesada, fecha_procesada 
                FROM alertas_sistema 
                WHERE procesada = 0
            '''
            params = []
            
            if nivel:
                query += ' AND nivel = ?'
                params.append(nivel)
            
            if ubicacion:
                query += ' AND ubicacion LIKE ?'
                params.append(f'%{ubicacion}%')
            
            query += ' ORDER BY timestamp DESC LIMIT ?'
            params.append(limite)
            
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            
            alertas = []
            for row in resultados:
                alerta = {
                    'id': row[0],
                    'timestamp': row[1],
                    'nivel': row[2],
                    'tipo': row[3],
                    'ubicacion': row[4],
                    'mensaje': row[5],
                    'datos_adicionales': json.loads(row[6]) if row[6] else {},
                    'procesada': row[7],
                    'fecha_procesada': row[8]
                }
                alertas.append(alerta)
            
            conn.close()
            return alertas
            
        except Exception as e:
            self.logger.error(f"Error obteniendo alertas pendientes: {e}")
            return []
    
    def marcar_alerta_procesada(self, alerta_id, comentario=""):
        """Marca una alerta como procesada CORRECTAMENTE"""
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            self.logger.error(f"Base de datos no encontrada: {db_path}")
            return False
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            fecha_procesada = datetime.now().isoformat()
            
            # Obtener datos actuales de la alerta
            cursor.execute('''
                SELECT datos_adicionales FROM alertas_sistema WHERE id = ?
            ''', (alerta_id,))
            
            resultado = cursor.fetchone()
            if not resultado:
                self.logger.error(f"Alerta con ID {alerta_id} no encontrada")
                conn.close()
                return False
            
            # Actualizar datos adicionales con comentario
            if resultado[0]:
                datos_adicionales = json.loads(resultado[0])
                datos_adicionales['comentario_procesado'] = comentario
                datos_adicionales['fecha_procesado'] = fecha_procesada
            else:
                datos_adicionales = {
                    'comentario_procesado': comentario,
                    'fecha_procesado': fecha_procesada
                }
            
            # Actualizar alerta como procesada
            cursor.execute('''
                UPDATE alertas_sistema 
                SET procesada = 1, 
                    fecha_procesada = ?,
                    datos_adicionales = ?
                WHERE id = ?
            ''', (
                fecha_procesada, 
                json.dumps(datos_adicionales, ensure_ascii=False), 
                alerta_id
            ))
            
            if cursor.rowcount == 0:
                self.logger.error(f"No se pudo actualizar alerta {alerta_id}")
                conn.close()
                return False
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Alerta {alerta_id} marcada como procesada: {comentario}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error marcando alerta como procesada: {e}")
            return False
    
    def generar_reporte_alertas(self, horas=24):
        """Genera un reporte de alertas de las ultimas N horas"""
        fecha_inicio = datetime.now().isoformat()
        
        db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        
        if not os.path.exists(db_path):
            self.logger.error(f"Base de datos no encontrada: {db_path}")
            return None
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Obtener estadisticas
            cursor.execute('''
                SELECT 
                    nivel,
                    tipo,
                    ubicacion,
                    COUNT(*) as cantidad,
                    SUM(CASE WHEN procesada = 1 THEN 1 ELSE 0 END) as procesadas
                FROM alertas_sistema
                WHERE datetime(timestamp) >= datetime('now', ?)
                GROUP BY nivel, tipo, ubicacion
                ORDER BY 
                    CASE nivel
                        WHEN 'CRITICA' THEN 1
                        WHEN 'ADVERTENCIA' THEN 2
                        WHEN 'INFO' THEN 3
                        ELSE 4
                    END,
                    cantidad DESC
            ''', (f'-{horas} hours',))
            
            resultados = cursor.fetchall()
            
            # Obtener alertas mas recientes
            cursor.execute('''
                SELECT timestamp, nivel, tipo, ubicacion, mensaje, procesada, fecha_procesada
                FROM alertas_sistema
                WHERE datetime(timestamp) >= datetime('now', ?)
                ORDER BY timestamp DESC
                LIMIT 10
            ''', (f'-{horas} hours',))
            
            alertas_recientes = cursor.fetchall()
            
            conn.close()
            
            # Construir reporte
            reporte = {
                'fecha_generacion': fecha_inicio,
                'periodo_horas': horas,
                'resumen_alertas': [],
                'alertas_recientes': [],
                'total_alertas': 0,
                'total_procesadas': 0
            }
            
            for nivel, tipo, ubicacion, cantidad, procesadas in resultados:
                reporte['resumen_alertas'].append({
                    'nivel': nivel,
                    'tipo': tipo,
                    'ubicacion': ubicacion,
                    'cantidad': cantidad,
                    'procesadas': procesadas,
                    'pendientes': cantidad - procesadas
                })
                reporte['total_alertas'] += cantidad
                reporte['total_procesadas'] += procesadas
            
            for timestamp, nivel, tipo, ubicacion, mensaje, procesada, fecha_procesada in alertas_recientes:
                reporte['alertas_recientes'].append({
                    'timestamp': timestamp,
                    'nivel': nivel,
                    'tipo': tipo,
                    'ubicacion': ubicacion,
                    'mensaje': mensaje[:100] + '...' if len(mensaje) > 100 else mensaje,
                    'procesada': bool(procesada),
                    'fecha_procesada': fecha_procesada
                })
            
            # Guardar reporte
            fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            reporte_path = os.path.join(self.proyecto_root, 'reports', f'reporte_alertas_{fecha_str}.json')
            
            with open(reporte_path, 'w', encoding='utf-8') as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Reporte de alertas generado: {reporte_path}")
            
            # Mostrar resumen en consola
            print(f"\n{'='*60}")
            print(f"REPORTE DE ALERTAS - Ultimas {horas} horas")
            print(f"{'='*60}")
            print(f"Total alertas: {reporte['total_alertas']}")
            print(f"Alertas procesadas: {reporte['total_procesadas']}")
            print(f"Alertas pendientes: {reporte['total_alertas'] - reporte['total_procesadas']}")
            print(f"\nResumen por tipo:")
            
            for item in reporte['resumen_alertas']:
                print(f"  {item['nivel']} - {item['tipo']} [{item['ubicacion']}]: {item['cantidad']} (Pendientes: {item['pendientes']})")
            
            if reporte['alertas_recientes']:
                print(f"\nUltimas alertas:")
                for alerta in reporte['alertas_recientes'][:3]:
                    estado = "PROCESADA" if alerta['procesada'] else "PENDIENTE"
                    print(f"  [{alerta['timestamp'][11:19]}] {alerta['nivel']}: {alerta['mensaje']} ({estado})")
            
            print(f"{'='*60}")
            
            return reporte
            
        except Exception as e:
            self.logger.error(f"Error generando reporte de alertas: {e}")
            return None

# Prueba del sistema
if __name__ == "__main__":
    print("PROBANDO SISTEMA DE ALERTAS CORREGIDO")
    print("="*60)
    
    # Crear sistema
    sistema = SistemaAlertas()
    
    # Probar insercion
    print("\n1. Creando alerta de prueba...")
    alerta = sistema.registrar_alerta(
        nivel=NivelAlerta.CRITICA,
        tipo=TipoAlerta.CALIDAD_AIRE,
        mensaje="CO2 critico: 1500 ppm",
        datos_adicionales={'co2': 1500, 'temperatura': 28.5},
        ubicacion="Aula 101"
    )
    
    print("\n2. Verificando alertas pendientes...")
    pendientes = sistema.obtener_alertas_pendientes()
    print(f"Alertas pendientes: {len(pendientes)}")
    
    if pendientes:
        print("\n3. Marcando primera alerta como procesada...")
        if sistema.marcar_alerta_procesada(pendientes[0]['id'], "Ventilacion activada"):
            print("Alerta marcada como procesada")
        else:
            print("Error marcando alerta")
    
    print("\n4. Generando reporte...")
    sistema.generar_reporte_alertas(horas=1)
    
    print("\n" + "="*60)
    print("PRUEBA COMPLETADA")
    print("="*60)