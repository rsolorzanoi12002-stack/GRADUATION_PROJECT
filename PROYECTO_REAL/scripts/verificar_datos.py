import os
import json
import sys

def verificar_estructura():
    """Verifica que toda la estructura del proyecto este correcta"""
    print("VERIFICANDO ESTRUCTURA DEL PROYECTO")
    print("=" * 50)
    
    # Obtener la ruta del directorio raiz del proyecto
    proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    directorios_requeridos = [
        'data/raw_json',
        'data/processed',
        'data/database',
        'data/archive',
        'scripts',
        'config',
        'logs',
        'notebooks',
        'models',
        'reports'
    ]
    
    # Verificar directorios con rutas absolutas
    for dir_relativo in directorios_requeridos:
        dir_path = os.path.join(proyecto_root, dir_relativo)
        if os.path.exists(dir_path):
            print(f"OK {dir_relativo}")
        else:
            print(f"NO {dir_relativo} - NO EXISTE")
            print(f"   Creando directorio...")
            os.makedirs(dir_path, exist_ok=True)
    
    print("=" * 50)
    
    # Verificar archivos JSON en raw_json
    raw_json_dir = os.path.join(proyecto_root, 'data/raw_json')
    if os.path.exists(raw_json_dir):
        archivos_json = [f for f in os.listdir(raw_json_dir) if f.endswith('.json')]
        print(f"Archivos JSON encontrados en data/raw_json: {len(archivos_json)}")
        
        for archivo in archivos_json:
            archivo_path = os.path.join(raw_json_dir, archivo)
            try:
                with open(archivo_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Verificar estructura basica
                    if 'sensor_data' in data:
                        print(f"   OK {archivo} - Estructura valida")
                        # Mostrar algunos datos
                        if 'readings' in data['sensor_data']:
                            readings = data['sensor_data']['readings']
                            if 'scd30' in readings:
                                co2 = readings['scd30'].get('co2', 'N/A')
                                print(f"      CO2: {co2} ppm")
                    else:
                        print(f"   NO {archivo} - Falta clave 'sensor_data'")
            except json.JSONDecodeError as e:
                print(f"   NO {archivo} - JSON invalido: {e}")
            except Exception as e:
                print(f"   NO {archivo} - Error: {e}")
    else:
        print(f"NO Directorio data/raw_json no existe")
    
    print("Verificacion completada")

if __name__ == "__main__":
    verificar_estructura()