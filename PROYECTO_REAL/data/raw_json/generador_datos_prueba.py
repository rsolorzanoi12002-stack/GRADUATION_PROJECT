import json
import random
import math
from datetime import datetime, timedelta
import os

class GeneradorDatosPruebaDiciembre2025:
    def __init__(self):
        self.ubicaciones = [
            "Campus Centenario - UPS Guayaquil",
            "Edificio de Ingeniería - Piso 3",
            "Biblioteca Central",
            "Laboratorio de Ciencias",
            "Cafetería Principal",
            "Área Deportiva",
            "Estacionamiento Norte",
            "Auditorio Central"
        ]
        
        self.dispositivos = [
            "UPS-SENSOR-001",
            "UPS-SENSOR-002", 
            "UPS-SENSOR-003",
            "UPS-SENSOR-004",
            "UPS-SENSOR-005"
        ]
        
        # Rangos realistas para CO2 (ppm)
        self.rangos_co2 = {
            "excelente": (350, 450),
            "buena": (451, 600),
            "moderada": (601, 800),
            "deficiente": (801, 1000),
            "muy_deficiente": (1001, 1200),
            "peligrosa": (1201, 2000)
        }
        
        # Patrón diario más realista
        self.patron_horario_co2 = {
            "madrugada": (0.7, 0.9),
            "mañana": (0.9, 1.2),
            "pico_mañana": (1.3, 1.6),
            "tarde": (1.1, 1.4),
            "noche": (0.8, 1.1),
        }
        
        # Patrón semanal más realista
        self.patron_semanal_co2 = {
            0: 1.3, 1: 1.2, 2: 1.2, 3: 1.3, 4: 1.1, 5: 0.7, 6: 0.6
        }
        
        # Factores por tipo de ubicación
        self.factores_ubicacion = {
            "Campus Centenario - UPS Guayaquil": 0.9,
            "Edificio de Ingeniería - Piso 3": 1.3,
            "Biblioteca Central": 1.2,
            "Laboratorio de Ciencias": 1.4,
            "Cafetería Principal": 1.5,
            "Área Deportiva": 0.8,
            "Estacionamiento Norte": 1.1,
            "Auditorio Central": 1.0
        }
        
        # Historial para simular inercia
        self.historial = {}
        
    def determinar_calidad_segun_fecha(self, fecha):
        dia_semana = fecha.weekday()
        dia_mes = fecha.day
        hora = fecha.hour
        
        if dia_semana < 5:  # Lunes a Viernes
            if 8 <= hora <= 16:
                calidades = ["excelente", "buena", "moderada", "deficiente", "muy_deficiente", "peligrosa"]
                pesos = [0.05, 0.20, 0.35, 0.25, 0.10, 0.05]
            elif 17 <= hora <= 21:
                calidades = ["excelente", "buena", "moderada", "deficiente", "muy_deficiente", "peligrosa"]
                pesos = [0.10, 0.25, 0.30, 0.20, 0.10, 0.05]
            else:
                calidades = ["excelente", "buena", "moderada", "deficiente", "muy_deficiente", "peligrosa"]
                pesos = [0.30, 0.40, 0.20, 0.07, 0.02, 0.01]
        else:  # Fin de semana
            if 10 <= hora <= 18:
                calidades = ["excelente", "buena", "moderada", "deficiente", "muy_deficiente", "peligrosa"]
                pesos = [0.20, 0.35, 0.25, 0.12, 0.05, 0.03]
            else:
                calidades = ["excelente", "buena", "moderada", "deficiente", "muy_deficiente", "peligrosa"]
                pesos = [0.40, 0.35, 0.15, 0.07, 0.02, 0.01]
        
        # Días festivos
        dias_festivos = [6, 7, 8, 24, 25, 31]
        if dia_mes in dias_festivos:
            pesos = [0.35, 0.35, 0.20, 0.07, 0.02, 0.01]
        
        # Fin de semestre (después del 15)
        if dia_mes > 15:
            pesos = [peso * 1.2 for peso in pesos[:2]] + pesos[2:]
            total = sum(pesos)
            pesos = [p/total for p in pesos]
        
        return random.choices(calidades, pesos)[0]
    
    def obtener_franja_horaria(self, hora):
        if 0 <= hora < 6:
            return "madrugada"
        elif 6 <= hora < 10:
            return "mañana"
        elif 10 <= hora < 14:
            return "pico_mañana"
        elif 14 <= hora < 18:
            return "tarde"
        else:
            return "noche"
    
    def generar_json_diciembre_2025(self, fecha_hora, ubicacion=None, dispositivo=None):
        calidad = self.determinar_calidad_segun_fecha(fecha_hora)
        co2_min, co2_max = self.rangos_co2[calidad]
        
        hora = fecha_hora.hour
        dia_semana = fecha_hora.weekday()
        dia_mes = fecha_hora.day
        
        franja = self.obtener_franja_horaria(hora)
        factor_horario_min, factor_horario_max = self.patron_horario_co2[franja]
        factor_horario = random.uniform(factor_horario_min, factor_horario_max)
        factor_semanal = self.patron_semanal_co2[dia_semana]
        
        ubicacion = ubicacion or random.choice(self.ubicaciones)
        dispositivo = dispositivo or random.choice(self.dispositivos)
        factor_ubicacion = self.factores_ubicacion.get(ubicacion, 1.0)
        
        # Generar CO2 con patrones
        co2_base = random.uniform(co2_min, co2_max)
        co2_ajustado = co2_base * factor_horario * factor_semanal * factor_ubicacion
        
        # Aplicar inercia
        key = f"{ubicacion}_{dispositivo}"
        if key in self.historial:
            co2_anterior = self.historial[key]
            cambio_max = co2_anterior * 0.2
            co2_ajustado = co2_anterior + max(-cambio_max, min(cambio_max, co2_ajustado - co2_anterior))
        
        self.historial[key] = co2_ajustado
        
        # Temperatura realista
        temp_base = 25.0 + 5.0 * math.sin((hora - 14) * math.pi / 12)
        temp_ajuste_co2 = (co2_ajustado - 400) / 1000 * 2.0
        temp_variacion_dia = random.uniform(-1.5, 1.5)
        
        if "Campus" in ubicacion or "Área" in ubicacion or "Estacionamiento" in ubicacion:
            temp_exterior_factor = 1.0
        else:
            temp_exterior_factor = 1.1
        
        temperatura = (temp_base + temp_ajuste_co2 + temp_variacion_dia) * temp_exterior_factor
        
        # Humedad realista
        humedad_base = 75.0
        humedad_variacion = 15.0 * math.sin((hora - 4) * math.pi / 12)
        humedad_calor_ajuste = -0.5 * (temperatura - 27.0)
        
        if "Laboratorio" in ubicacion or "Cafetería" in ubicacion:
            humedad_ubicacion = random.uniform(5.0, 10.0)
        else:
            humedad_ubicacion = random.uniform(-5.0, 5.0)
        
        humedad = humedad_base + humedad_variacion + humedad_calor_ajuste + humedad_ubicacion
        humedad = max(40.0, min(95.0, humedad))
        
        # Presión atmosférica
        presion_base = 1013.25
        presion_diurna = 0.5 * math.sin(hora * math.pi / 12)
        presion_mensual = 2.0 * math.sin(dia_mes * 2 * math.pi / 31)
        presion = presion_base + presion_diurna + presion_mensual + random.uniform(-1.0, 1.0)
        
        # MQ135
        mq135_base = (co2_ajustado - 400) * 0.15
        
        if random.random() < 0.15:
            mq135_base += random.uniform(10.0, 40.0)
        
        ruido_sensor = random.normalvariate(0.0, 2.0)
        mq135_analog = max(0, mq135_base + ruido_sensor)
        mq135_digital = 1 if co2_ajustado > 1000 or mq135_analog > 100 else 0
        
        # Batería
        dia_del_mes = fecha_hora.day
        bateria_base = 98 - (dia_del_mes * 0.3)
        
        if 8 <= hora <= 20:
            bateria_base -= random.uniform(0.1, 0.3)
        
        if random.random() < 0.05:
            bateria_base += random.uniform(10.0, 30.0)
        
        bateria = max(25, min(100, int(bateria_base + random.uniform(-3.0, 3.0))))
        
        # Coordenadas
        coordenadas = {
            "Campus Centenario - UPS Guayaquil": (-2.170998, -79.922356),
            "Edificio de Ingeniería - Piso 3": (-2.171200, -79.922100),
            "Biblioteca Central": (-2.170800, -79.922500),
            "Laboratorio de Ciencias": (-2.171000, -79.921800),
            "Cafetería Principal": (-2.170500, -79.922300),
            "Área Deportiva": (-2.172000, -79.922000),
            "Estacionamiento Norte": (-2.171500, -79.921500),
            "Auditorio Central": (-2.170300, -79.922700)
        }
        
        lat, lon = coordenadas.get(ubicacion, (-2.170998, -79.922356))
        
        # JSON
        datos = {
            "sensor_data": {
                "metadata": {
                    "device_id": dispositivo,
                    "location": ubicacion,
                    "timestamp": fecha_hora.isoformat(),
                    "latitude": lat,
                    "longitude": lon
                },
                "readings": {
                    "scd30": {
                        "co2": round(co2_ajustado, 2),
                        "temperature": round(temperatura, 2),
                        "humidity": round(humedad, 2)
                    },
                    "bme280": {
                        "temperature": round(temperatura + random.uniform(-0.3, 0.3), 2),
                        "humidity": round(humedad + random.uniform(-1.5, 1.5), 2),
                        "pressure": round(presion, 2)
                    },
                    "mq135": {
                        "analog_value": round(mq135_analog, 2),
                        "digital_value": mq135_digital
                    }
                },
                "system_info": {
                    "battery_level": bateria,
                    "sampling_interval": 3600
                }
            }
        }
        
        return datos, calidad
    
    def generar_datos_diciembre_2025(self, total_registros=400):
        """Genera 400 registros distribuidos en diciembre 2025"""
        print(f"Generando {total_registros} registros para diciembre 2025...")
        print("-" * 60)
        
        # Fechas base: 1 al 31 de diciembre 2025
        fecha_inicio = datetime(2025, 12, 1)
        fecha_fin = datetime(2025, 12, 31, 23, 59, 59)
        dias_totales = (fecha_fin - fecha_inicio).days + 1
        
        # Distribuir registros de forma desigual (más en días laborables)
        registros_por_dia = {}
        registros_restantes = total_registros
        
        # Distribución base
        for i in range(dias_totales):
            fecha = fecha_inicio + timedelta(days=i)
            dia_semana = fecha.weekday()
            if dia_semana < 5:  # Días laborables más registros
                base = 15
            else:  # Fin de semana menos registros
                base = 8
            registros_por_dia[fecha.date()] = base
            registros_restantes -= base
        
        # Distribuir el resto aleatoriamente
        while registros_restantes > 0:
            dia_aleatorio = random.randint(0, dias_totales - 1)
            fecha = fecha_inicio + timedelta(days=dia_aleatorio)
            registros_por_dia[fecha.date()] += 1
            registros_restantes -= 1
        
        # Generar datos para cada día
        todos_datos = []
        resumen_calidades = {}
        
        for fecha_str, cantidad in registros_por_dia.items():
            fecha = datetime.combine(fecha_str, datetime.min.time())
            dia_semana = fecha.weekday()
            
            for i in range(cantidad):
                # Horas según día de la semana
                if dia_semana < 5:  # Laborables
                    if i < cantidad * 0.7:  # 70% en horario laboral
                        hora = random.randint(8, 18)
                    else:  # 30% fuera de horario
                        hora = random.choice([6, 7, 19, 20, 21, 22])
                else:  # Fin de semana
                    hora = random.randint(9, 20)
                
                minuto = random.choice([0, 15, 30, 45])
                segundo = random.randint(0, 59)
                fecha_hora = fecha.replace(hour=hora, minute=minuto, second=segundo)
                
                ubicacion = random.choice(self.ubicaciones)
                dispositivo = random.choice(self.dispositivos)
                
                datos, calidad = self.generar_json_diciembre_2025(
                    fecha_hora=fecha_hora,
                    ubicacion=ubicacion,
                    dispositivo=dispositivo
                )
                
                todos_datos.append((fecha_hora, datos, calidad))
                resumen_calidades[calidad] = resumen_calidades.get(calidad, 0) + 1
        
        todos_datos.sort(key=lambda x: x[0])
        
        print(f"Registros generados: {len(todos_datos)}")
        print("\nDistribución por días:")
        for fecha_str, cantidad in sorted(registros_por_dia.items()):
            print(f"  {fecha_str}: {cantidad} registros")
        
        print("\nDistribución por calidad:")
        for calidad, cantidad in resumen_calidades.items():
            porcentaje = (cantidad / total_registros) * 100
            print(f"  {calidad}: {cantidad} registros ({porcentaje:.1f}%)")
        
        print("-" * 60)
        
        return [datos for _, datos, _ in todos_datos]
    
    def guardar_json(self, datos, directorio="raw_json", prefijo="sensor_data"):
        """Guarda un JSON en archivo en la ruta especificada"""
        # RUTA ESPECÍFICA QUE PIDES
        ruta_base = r"C:\Users\solorzanor\Documents\PROYECTO_REAL\data"
        ruta_completa = os.path.join(ruta_base, directorio)
        
        os.makedirs(ruta_completa, exist_ok=True)
        
        timestamp_str = datos["sensor_data"]["metadata"]["timestamp"]
        timestamp = datetime.fromisoformat(timestamp_str)
        
        nombre_archivo = f"{prefijo}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        ruta_archivo = os.path.join(ruta_completa, nombre_archivo)
        
        with open(ruta_archivo, 'w', encoding='utf-8') as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
        
        return nombre_archivo, ruta_archivo
    
    def generar_y_guardar_diciembre_2025(self, total_registros=400, directorio="raw_json"):
        """Genera y guarda los registros de diciembre 2025"""
        print("=" * 70)
        print("GENERADOR DE DATOS REALISTAS - DICIEMBRE 2025")
        print("=" * 70)
        print(f"Generando {total_registros} registros distribuidos en diciembre 2025...")
        print("Período: 01/12/2025 al 31/12/2025")
        print("=" * 70)
        
        # Ruta específica
        ruta_base = r"C:\Users\solorzanor\Documents\PROYECTO_REAL\data"
        ruta_completa = os.path.join(ruta_base, directorio)
        
        # Limpiar directorio si existe
        if os.path.exists(ruta_completa):
            archivos_existentes = [f for f in os.listdir(ruta_completa) if f.endswith('.json')]
            if archivos_existentes:
                print(f"\n⚠️  ADVERTENCIA: Ya existen {len(archivos_existentes)} archivos en {ruta_completa}")
                respuesta = input("¿Desea eliminarlos antes de generar nuevos? (s/N): ").strip().lower()
                if respuesta == 's':
                    for archivo in archivos_existentes:
                        os.remove(os.path.join(ruta_completa, archivo))
                    print(f"  → Eliminados {len(archivos_existentes)} archivos existentes")
        
        # Generar datos
        datos_lista = self.generar_datos_diciembre_2025(total_registros)
        
        print(f"\nGuardando {len(datos_lista)} archivos JSON...")
        print(f"Ruta: {ruta_completa}")
        print("-" * 50)
        
        archivos_creados = []
        rutas_creadas = []
        progreso_total = len(datos_lista)
        
        for i, datos in enumerate(datos_lista, 1):
            nombre, ruta = self.guardar_json(datos, directorio)
            archivos_creados.append(nombre)
            rutas_creadas.append(ruta)
            
            if i % 50 == 0 or i == progreso_total:
                porcentaje = (i / progreso_total) * 100
                print(f"  [{i:3d}/{progreso_total}] {porcentaje:5.1f}% - {nombre}")
        
        print("-" * 50)
        
        # Estadísticas finales
        print("\n" + "=" * 70)
        print("✅ GENERACIÓN COMPLETADA")
        print("=" * 70)
        
        fechas = []
        ubicaciones_unicas = set()
        dispositivos_unicos = set()
        
        for datos in datos_lista:
            timestamp = datos["sensor_data"]["metadata"]["timestamp"]
            fecha = datetime.fromisoformat(timestamp).date()
            fechas.append(fecha)
            ubicaciones_unicas.add(datos["sensor_data"]["metadata"]["location"])
            dispositivos_unicos.add(datos["sensor_data"]["metadata"]["device_id"])
        
        fecha_min = min(fechas)
        fecha_max = max(fechas)
        
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"   • Total registros: {len(datos_lista)}")
        print(f"   • Período: {fecha_min} a {fecha_max}")
        print(f"   • Días cubiertos: {(fecha_max - fecha_min).days + 1} días")
        print(f"   • Ubicaciones diferentes: {len(ubicaciones_unicas)}")
        print(f"   • Dispositivos diferentes: {len(dispositivos_unicos)}")
        print(f"   • Ruta de guardado: {ruta_completa}")
        
        print(f"\n📅 EJEMPLOS DE ARCHIVOS GENERADOS:")
        for i, archivo in enumerate(archivos_creados[:5], 1):
            print(f"   {i}. {archivo}")
        if len(archivos_creados) > 5:
            print(f"   ... y {len(archivos_creados) - 5} más")
        
        print(f"\n⏰ PRIMER REGISTRO: {archivos_creados[0]}")
        print(f"   ÚLTIMO REGISTRO: {archivos_creados[-1]}")
        
        # Verificar que los archivos existen
        archivos_existentes = [f for f in os.listdir(ruta_completa) if f.endswith('.json')]
        print(f"\n📁 VERIFICACIÓN: {len(archivos_existentes)} archivos en la carpeta")
        
        print("=" * 70)
        
        return archivos_creados

def main():
    """Función principal"""
    print("=" * 70)
    print("GENERADOR DE DATOS REALISTAS - DICIEMBRE 2025")
    print("Universidad Politécnica Salesiana - Campus Centenario")
    print("=" * 70)
    print("\nEste generador creará 400 registros REALISTAS de calidad del aire")
    print("distribuidos en diciembre 2025.")
    print("\nCaracterísticas REALISTAS:")
    print("  • 400 registros en total")
    print("  • Período: 1 al 31 de diciembre 2025")
    print("  • Patrones horarios y semanales realistas")
    print("  • Inercia en los datos (no cambios bruscos)")
    print("  • Correlaciones naturales entre variables")
    print("  • Guardado en: C:\\Users\\solorzanor\\Documents\\PROYECTO_REAL\\data\\raw_json")
    print("=" * 70)
    
    try:
        print("\nOPCIONES:")
        print("  1. Generar 400 registros (recomendado)")
        print("  2. Personalizar cantidad (200-600)")
        print("  3. Ver ruta y detalles")
        
        opcion = input("\nSeleccione opción (1-3): ").strip()
        
        generador = GeneradorDatosPruebaDiciembre2025()
        
        if opcion == "1":
            print("\n" + "=" * 70)
            print("GENERANDO 400 REGISTROS REALISTAS PARA DICIEMBRE 2025")
            print("=" * 70)
            archivos = generador.generar_y_guardar_diciembre_2025(400)
        
        elif opcion == "2":
            try:
                cantidad = int(input("Ingrese cantidad de registros (200-600): "))
                cantidad = max(200, min(600, cantidad))
                print(f"\nGenerando {cantidad} registros para diciembre 2025...")
                archivos = generador.generar_y_guardar_diciembre_2025(cantidad)
            except ValueError:
                print("Error: Ingrese un número válido. Usando 400 por defecto.")
                archivos = generador.generar_y_guardar_diciembre_2025(400)
        
        elif opcion == "3":
            ruta = r"C:\Users\solorzanor\Documents\PROYECTO_REAL\data\raw_json"
            print(f"\nRuta de guardado: {ruta}")
            
            if os.path.exists(ruta):
                archivos = [f for f in os.listdir(ruta) if f.endswith('.json')]
                print(f"Archivos existentes: {len(archivos)}")
                if archivos:
                    print("Primeros 5 archivos:")
                    for i, archivo in enumerate(archivos[:5], 1):
                        print(f"  {i}. {archivo}")
            else:
                print("La carpeta no existe. Se creará al generar datos.")
            
            print("\nPresiona Enter para generar datos...")
            input()
            print("\nGenerando 400 registros...")
            archivos = generador.generar_y_guardar_diciembre_2025(400)
        
        else:
            print("Opción no válida. Generando 400 registros por defecto...")
            archivos = generador.generar_y_guardar_diciembre_2025(400)
        
        if opcion in ["1", "2", "3"]:
            ruta = r"C:\Users\solorzanor\Documents\PROYECTO_REAL\data\raw_json"
            print(f"\n🎉 ¡GENERACIÓN COMPLETADA!")
            print(f"\nLos 400 archivos JSON están guardados en:")
            print(f"{ruta}")
            print(f"\nPuedes abrir esta ruta en el Explorador de Windows.")
            
    except KeyboardInterrupt:
        print("\n\nGeneración cancelada por el usuario.")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()