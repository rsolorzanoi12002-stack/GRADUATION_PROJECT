# archivo: limpiar_base_datos.py
import os
import sqlite3
import shutil

def limpiar_base_datos_completamente():
    """Limpia toda la base de datos y la deja como nueva"""
    
    print("="*60)
    print("LIMPIADOR DE BASE DE DATOS - SISTEMA CALIDAD AIRE")
    print("="*60)
    
    # Ruta de la base de datos
    db_path = "data/database/calidad_aire.db"
    backup_path = "calidad_aire_backup.db"
    
    # 1. Hacer backup de la base de datos actual
    if os.path.exists(db_path):
        shutil.copy2(db_path, backup_path)
        print(f"✓ Backup creado: {backup_path}")
    else:
        print("ℹ️  Base de datos no encontrada, se creará nueva")
    
    # 2. Eliminar base de datos actual
    if os.path.exists(db_path):
        os.remove(db_path)
        print("✓ Base de datos eliminada")
    
    # 3. Crear estructura nueva
    print("\nCreando estructura nueva de base de datos...")
    
    # Asegurar que existe el directorio
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 4. Crear tabla sensor_requests (JSON originales)
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
    print("✓ Tabla 'sensor_requests' creada")
    
    # 5. Crear tabla sensor_responses (análisis)
    cursor.execute('''
        CREATE TABLE sensor_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER,
            calidad_aire_pred TEXT,
            co2_nivel TEXT,
            temperatura REAL,
            humedad REAL,
            presion REAL,
            importancia_variables TEXT,
            prediccion_detalle TEXT,
            created_at TEXT,
            FOREIGN KEY (request_id) REFERENCES sensor_requests(id)
        )
    ''')
    print("✓ Tabla 'sensor_responses' creada")
    
    # 6. Crear tabla alertas_sistema CORRECTA
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
    print("✓ Tabla 'alertas_sistema' creada (estructura correcta)")
    
    # 7. Crear índices para mejor performance
    cursor.execute('CREATE INDEX idx_requests_timestamp ON sensor_requests(timestamp)')
    cursor.execute('CREATE INDEX idx_responses_created ON sensor_responses(created_at)')
    cursor.execute('CREATE INDEX idx_alertas_timestamp ON alertas_sistema(timestamp)')
    cursor.execute('CREATE INDEX idx_alertas_procesada ON alertas_sistema(procesada)')
    cursor.execute('CREATE INDEX idx_alertas_nivel ON alertas_sistema(nivel)')
    print("✓ Índices creados")
    
    conn.commit()
    conn.close()
    
    print("\n" + "="*60)
    print("✅ BASE DE DATOS LIMPIA Y LISTA PARA USAR")
    print("="*60)
    print("\nEstructura creada:")
    print("  • sensor_requests     - JSON originales de sensores")
    print("  • sensor_responses    - Análisis y predicciones")
    print("  • alertas_sistema     - Alertas del sistema (estructura correcta)")
    print("\nNota: El backup está en data/database/calidad_aire_backup.db")
    print("="*60)

def verificar_estructura():
    """Verifica la estructura de la base de datos"""
    db_path = "data/database/calidad_aire.db"
    
    if not os.path.exists(db_path):
        print("❌ Base de datos no encontrada")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("VERIFICACIÓN DE ESTRUCTURA")
    print("="*60)
    
    # Ver tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tablas = cursor.fetchall()
    
    print(f"\nTablas encontradas ({len(tablas)}):")
    for tabla in tablas:
        print(f"  • {tabla[0]}")
        
        # Ver estructura de cada tabla
        cursor.execute(f"PRAGMA table_info({tabla[0]})")
        columnas = cursor.fetchall()
        
        print(f"    Columnas ({len(columnas)}):")
        for col in columnas:
            not_null = " NOT NULL" if col[3] else ""
            default = f" DEFAULT {col[4]}" if col[4] else ""
            print(f"      - {col[1]} ({col[2]}{not_null}{default})")
    
    conn.close()
    
    print("\n" + "="*60)
    print("VERIFICACIÓN COMPLETADA")
    print("="*60)

if __name__ == "__main__":
    print("¿Está seguro de limpiar completamente la base de datos?")
    print("Se perderán todos los datos existentes.")
    print("Se creará un backup automáticamente.")
    
    respuesta = input("\nContinuar? (si/no): ").strip().lower()
    
    if respuesta == 'si':
        limpiar_base_datos_completamente()
        verificar_estructura()
    else:
        print("Operación cancelada.")