import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import json
import matplotlib

class DashboardCalidadAire:
    def __init__(self):
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_path = os.path.join(self.proyecto_root, 'data/database/calidad_aire.db')
        self.fig = None
        self.colores_calidad = {
            'Excelente': '#00FF00',    # Verde
            'Buena': '#90EE90',        # Verde claro
            'Moderada': '#FFFF00',     # Amarillo
            'Deficiente': '#FFA500',   # Naranja
            'Muy deficiente': '#FF4500', # Naranja rojizo
            'Peligrosa': '#FF0000'     # Rojo
        }
        
    def conectar_db(self):
        """Conecta a la base de datos"""
        if not os.path.exists(self.db_path):
            print(f"Error: Base de datos no encontrada en {self.db_path}")
            return None
        return sqlite3.connect(self.db_path)
    
    def verificar_estructura_db(self):
        """Verifica la estructura de la base de datos para diagnóstico"""
        print("\n" + "="*60)
        print("DIAGNÓSTICO COMPLETO DE BASE DE DATOS")
        print("="*60)
        
        with self.conectar_db() as conn:
            if conn is None:
                print("No se pudo conectar a la base de datos")
                return
            
            cursor = conn.cursor()
            
            # Ver todas las tablas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tablas = cursor.fetchall()
            print(f"\nTablas encontradas: {[t[0] for t in tablas]}")
            
            # Ver estructura de sensor_responses
            cursor.execute("PRAGMA table_info(sensor_responses)")
            columnas = cursor.fetchall()
            print("\nEstructura de tabla sensor_responses:")
            for col in columnas:
                print(f"  {col[1]} - {col[2]}")
            
            # Ver conteo de datos
            cursor.execute("SELECT COUNT(*) FROM sensor_responses")
            total_muestras = cursor.fetchone()[0]
            print(f"\nTotal muestras en sensor_responses: {total_muestras}")
            
            # Ver rango de fechas
            cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM sensor_responses")
            min_max = cursor.fetchone()
            print(f"\nRango de fechas en los datos:")
            print(f"  Inicio: {min_max[0]}")
            print(f"  Fin: {min_max[1]}")
            
            # Ver 5 ejemplos recientes
            print(f"\n5 mediciones más recientes:")
            
            # Primero verificar qué columnas existen
            cursor.execute("PRAGMA table_info(sensor_responses)")
            columnas_info = cursor.fetchall()
            columnas_nombres = [col[1] for col in columnas_info]
            
            # Construir consulta con columnas correctas
            if 'temperature' in columnas_nombres:
                query_ejemplos = '''
                    SELECT created_at, calidad_aire_pred, co2_nivel, temperature, humedad 
                    FROM sensor_responses 
                    ORDER BY created_at DESC 
                    LIMIT 5
                '''
            elif 'temperatura' in columnas_nombres:
                query_ejemplos = '''
                    SELECT created_at, calidad_aire_pred, co2_nivel, temperatura, humedad 
                    FROM sensor_responses 
                    ORDER BY created_at DESC 
                    LIMIT 5
                '''
            else:
                query_ejemplos = '''
                    SELECT created_at, calidad_aire_pred, co2_nivel 
                    FROM sensor_responses 
                    ORDER BY created_at DESC 
                    LIMIT 5
                '''
            
            cursor.execute(query_ejemplos)
            for i, row in enumerate(cursor.fetchall()):
                if len(row) >= 5:
                    fecha_hora = row[0]
                    fecha = fecha_hora[:10] if len(fecha_hora) >= 10 else fecha_hora
                    hora = fecha_hora[11:19] if len(fecha_hora) >= 19 else ""
                    print(f"  {i+1}. {fecha} {hora} - Calidad: {row[1]}, CO2: {row[2]}, Temp: {row[3]:.1f}°C")
                elif len(row) >= 3:
                    fecha_hora = row[0]
                    fecha = fecha_hora[:10] if len(fecha_hora) >= 10 else fecha_hora
                    hora = fecha_hora[11:19] if len(fecha_hora) >= 19 else ""
                    print(f"  {i+1}. {fecha} {hora} - Calidad: {row[1]}, CO2: {row[2]}")
                else:
                    print(f"  {i+1}. {row}")
        
        print("="*60)
    
    def obtener_ultimos_registros(self, limite=50):
        """Obtiene los últimos N registros (para modo histórico)"""
        try:
            with self.conectar_db() as conn:
                if conn is None:
                    return pd.DataFrame()
                
                print(f"[DEBUG] Obteniendo últimos {limite} registros...")
                
                # Primero verificar nombres de columnas
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(sensor_responses)")
                columnas_info = cursor.fetchall()
                columnas_nombres = [col[1] for col in columnas_info]
                
                # Determinar nombre de la columna de temperatura
                col_temperatura = 'temperatura'
                if 'temperature' in columnas_nombres:
                    col_temperatura = 'temperature'
                elif 'temperatura' in columnas_nombres:
                    col_temperatura = 'temperatura'
                
                # Consulta para últimos N registros
                query = f'''
                    SELECT 
                        s.created_at as timestamp,
                        s.calidad_aire_pred,
                        s.co2_nivel,
                        s.{col_temperatura} as temperatura,
                        s.humedad,
                        s.presion,
                        s.prediccion_detalle
                    FROM sensor_responses s
                    ORDER BY s.created_at DESC
                    LIMIT {limite}
                '''
                
                df = pd.read_sql_query(query, conn)
                
                if not df.empty:
                    print(f"[DEBUG] Encontrados {len(df)} registros")
                    
                    # Convertir timestamp
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    # Ordenar cronológicamente
                    df = df.sort_values('timestamp')
                    
                    # Extraer predicción
                    def extraer_prediccion(valor):
                        try:
                            if pd.isna(valor):
                                return 0.5
                            if isinstance(valor, (int, float)):
                                return float(valor)
                            if isinstance(valor, str):
                                try:
                                    data = json.loads(valor)
                                    if isinstance(data, dict):
                                        return data.get('prediccion_valor', 0.5)
                                    return float(data)
                                except:
                                    try:
                                        return float(valor)
                                    except:
                                        return 0.5
                            return 0.5
                        except:
                            return 0.5
                    
                    df['prediccion_valor'] = df['prediccion_detalle'].apply(extraer_prediccion)
                    
                    # Calcular índice de calidad
                    def calcular_indice_calidad(categoria):
                        indices = {
                            'Excelente': 0.9,
                            'Buena': 0.7,
                            'Moderada': 0.5,
                            'Deficiente': 0.3,
                            'Muy deficiente': 0.1,
                            'Peligrosa': 0.0
                        }
                        return indices.get(categoria, 0.5)
                    
                    df['indice_calidad'] = df['calidad_aire_pred'].apply(calcular_indice_calidad)
                    
                    # Calcular fecha mínima y máxima
                    fecha_min = df['timestamp'].min().strftime('%Y-%m-%d')
                    fecha_max = df['timestamp'].max().strftime('%Y-%m-%d')
                    print(f"[DEBUG] Rango de fechas: {fecha_min} a {fecha_max}")
                
                return df
                
        except Exception as e:
            print(f"[ERROR] Error obteniendo registros: {e}")
            return pd.DataFrame()
    
    def obtener_estadisticas_completas(self):
        """Obtiene estadísticas de TODOS los datos"""
        try:
            with self.conectar_db() as conn:
                if conn is None:
                    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(sensor_responses)")
                columnas_info = cursor.fetchall()
                columnas_nombres = [col[1] for col in columnas_info]
                
                col_temperatura = 'temperatura'
                if 'temperature' in columnas_nombres:
                    col_temperatura = 'temperature'
                elif 'temperatura' in columnas_nombres:
                    col_temperatura = 'temperatura'
                
                # Conteo por categoría (de TODOS los datos)
                query_categorias = '''
                    SELECT 
                        CASE 
                            WHEN calidad_aire_pred IN ('Excelente', 'Buena', 'Moderada', 'Deficiente', 'Muy deficiente', 'Peligrosa') 
                            THEN calidad_aire_pred
                            ELSE 'Desconocida'
                        END as categoria,
                        COUNT(*) as cantidad
                    FROM sensor_responses
                    GROUP BY categoria
                    ORDER BY 
                        CASE categoria
                            WHEN 'Excelente' THEN 1
                            WHEN 'Buena' THEN 2
                            WHEN 'Moderada' THEN 3
                            WHEN 'Deficiente' THEN 4
                            WHEN 'Muy deficiente' THEN 5
                            WHEN 'Peligrosa' THEN 6
                            ELSE 7
                        END
                '''
                df_categorias = pd.read_sql_query(query_categorias, conn)
                
                # Estadísticas generales (de TODOS los datos)
                query_estadisticas = f'''
                    SELECT 
                        COUNT(*) as total_muestras,
                        COALESCE(AVG({col_temperatura}), 0) as temp_promedio,
                        COALESCE(AVG(humedad), 0) as humedad_promedio,
                        COALESCE(AVG(presion), 0) as presion_promedio,
                        MIN(created_at) as fecha_min,
                        MAX(created_at) as fecha_max
                    FROM sensor_responses
                '''
                df_estadisticas = pd.read_sql_query(query_estadisticas, conn)
                
                # Última medición
                query_ultima = f'''
                    SELECT 
                        calidad_aire_pred, 
                        COALESCE(co2_nivel, 'Normal') as co2_nivel,
                        COALESCE({col_temperatura}, 0) as temperatura,
                        COALESCE(humedad, 0) as humedad,
                        created_at
                    FROM sensor_responses
                    WHERE created_at IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                '''
                df_ultima = pd.read_sql_query(query_ultima, conn)
                
                return df_categorias, df_estadisticas, df_ultima
                
        except Exception as e:
            print(f"[ERROR] Error obteniendo estadísticas: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def crear_grafico_series_tiempo(self, ax, df, titulo):
        """Crea gráfico de series de tiempo - MODIFICADO para datos históricos"""
        if df.empty:
            ax.text(0.5, 0.5, 'No hay datos disponibles', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Sin datos')
            ax.set_facecolor('#f8f8f8')
            return None
        
        print(f"[DEBUG] Creando gráfico de series con {len(df)} puntos")
        
        # Verificar que tenemos la columna temperatura
        if 'temperatura' not in df.columns:
            print(f"[DEBUG] ERROR: No hay columna 'temperatura' en los datos")
            ax.text(0.5, 0.5, 'Error: Datos incompletos', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Error de datos')
            return None
        
        # Crear colores para cada punto basado en calidad
        colores = df['calidad_aire_pred'].map(self.colores_calidad).fillna('#808080')
        
        # Usar índice de calidad
        valores_y = df['indice_calidad']
        
        # Gráfico de puntos
        scatter = ax.scatter(df['timestamp'], valores_y, 
                           c=colores, s=100, alpha=0.7, edgecolors='black', zorder=3)
        
        # Línea de conexión si hay más de 1 punto
        if len(df) > 1:
            ax.plot(df['timestamp'], valores_y, 'k-', alpha=0.3, linewidth=1, zorder=2)
        
        # Línea de tendencia si hay suficientes puntos
        if len(df) > 2:
            try:
                # Convertir tiempos a números para regresión
                tiempos_numericos = matplotlib.dates.date2num(df['timestamp'])
                z = np.polyfit(tiempos_numericos, valores_y, 1)
                p = np.poly1d(z)
                ax.plot(df['timestamp'], p(tiempos_numericos), 'b--', 
                       alpha=0.5, linewidth=2, label='Tendencia', zorder=1)
            except Exception as e:
                print(f"[DEBUG] Error calculando tendencia: {e}")
        
        ax.set_xlabel('Fecha', fontweight='bold')
        ax.set_ylabel('Índice de Calidad', fontweight='bold')
        ax.set_title(titulo, fontweight='bold')
        ax.grid(True, alpha=0.3, zorder=0)
        
        # Configurar límites del eje Y
        ax.set_ylim([0, 1])
        
        # Formatear eje x según el rango de fechas
        if len(df) > 1:
            # Calcular diferencia de tiempo
            time_diff = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).total_seconds()
            
            if time_diff > 30*86400:  # Más de 30 días
                date_format = '%d/%m/%Y'
            elif time_diff > 7*86400:  # Más de 7 días
                date_format = '%d/%m'
            elif time_diff > 86400:  # Más de 1 día
                date_format = '%d/%m %H:%M'
            else:  # Menos de 1 día
                date_format = '%H:%M:%S'
            
            ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter(date_format))
        else:
            ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%d/%m/%Y %H:%M'))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Agregar leyenda si hay puntos
        if not df.empty:
            handles = []
            labels = []
            seen_categories = set()
            
            for cat, color in self.colores_calidad.items():
                if cat in df['calidad_aire_pred'].values:
                    handles.append(plt.Line2D([0], [0], marker='o', color='w', 
                                            markerfacecolor=color, markersize=10))
                    labels.append(cat)
                    seen_categories.add(cat)
            
            # Agregar categorías no definidas
            other_cats = set(df['calidad_aire_pred'].unique()) - seen_categories
            if other_cats:
                handles.append(plt.Line2D([0], [0], marker='o', color='w', 
                                        markerfacecolor='#808080', markersize=10))
                labels.append('Otras')
            
            if handles:
                ax.legend(handles, labels, loc='upper left', 
                         bbox_to_anchor=(1.02, 1), title="Categorías")
        
        return scatter
    
    def crear_grafico_categorias(self, ax, df_categorias, titulo):
        """Crea gráfico de barras de categorías - MODIFICADO"""
        if df_categorias.empty or df_categorias['cantidad'].sum() == 0:
            ax.text(0.5, 0.5, 'No hay datos de categorías', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Sin datos')
            ax.set_facecolor('#f8f8f8')
            return None
        
        print(f"[DEBUG] Creando gráfico de categorías con {len(df_categorias)} categorías")
        
        # Ordenar categorías
        orden_categorias = ['Excelente', 'Buena', 'Moderada', 'Deficiente', 'Muy deficiente', 'Peligrosa']
        
        # Crear DataFrame ordenado
        df_ordenado = pd.DataFrame()
        for cat in orden_categorias:
            if cat in df_categorias['categoria'].values:
                cantidad = df_categorias.loc[df_categorias['categoria'] == cat, 'cantidad'].iloc[0]
                df_ordenado = pd.concat([df_ordenado, pd.DataFrame({'categoria': [cat], 'cantidad': [cantidad]})])
        
        # Agregar categorías no definidas al final
        otras_cats = df_categorias[~df_categorias['categoria'].isin(orden_categorias)]
        if not otras_cats.empty:
            for _, row in otras_cats.iterrows():
                df_ordenado = pd.concat([df_ordenado, 
                                        pd.DataFrame({'categoria': [row['categoria']], 
                                                    'cantidad': [row['cantidad']]})])
        
        # Crear colores
        colores = []
        for cat in df_ordenado['categoria']:
            if cat in self.colores_calidad:
                colores.append(self.colores_calidad[cat])
            else:
                colores.append('#808080')  # Gris para categorías desconocidas
        
        # Gráfico de barras
        bars = ax.bar(df_ordenado['categoria'], df_ordenado['cantidad'], 
                     color=colores, edgecolor='black', linewidth=1.5)
        
        ax.set_xlabel('Categoría de Calidad', fontweight='bold')
        ax.set_ylabel('Cantidad de Muestras', fontweight='bold')
        ax.set_title(titulo, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y', zorder=0)
        
        # Agregar valores en las barras
        total_muestras = df_ordenado['cantidad'].sum()
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                porcentaje = (height / total_muestras) * 100
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                       f'{int(height)}\n({porcentaje:.1f}%)', 
                       ha='center', va='bottom', fontsize=9)
        
        # Rotar etiquetas del eje X si son largas
        if len(df_ordenado) > 4:
            plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        
        # Mostrar total
        ax.text(0.02, 0.98, f'Total: {total_muestras} muestras', 
               transform=ax.transAxes, fontsize=10, 
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        return bars
    
    def crear_grafico_importancia(self, ax, df, titulo):
        """Crea gráfico de importancia de variables - MODIFICADO"""
        if df.empty or len(df) < 3:
            ax.text(0.5, 0.5, f'Insuficientes datos para análisis\n({len(df)} muestras)', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Datos insuficientes')
            ax.set_facecolor('#f8f8f8')
            return None
        
        print(f"[DEBUG] Creando gráfico de importancia con {len(df)} muestras")
        
        # Verificar columnas requeridas
        columnas_requeridas = ['temperatura', 'humedad', 'presion']
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
        
        if columnas_faltantes:
            print(f"[DEBUG] ADVERTENCIA: Faltan columnas para análisis: {columnas_faltantes}")
            ax.text(0.5, 0.5, f'Datos incompletos\nFaltan: {", ".join(columnas_faltantes)}', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Datos incompletos')
            return None
        
        try:
            # Calcular correlaciones reales si tenemos datos suficientes
            if len(df) >= 5:
                # Preparar datos para correlación
                variables_data = {
                    'CO2_Impacto': df['indice_calidad'],
                    'Temperatura': df['temperatura'],
                    'Humedad': df['humedad'],
                    'Presión': df['presion']
                }
                
                # Calcular correlación con el índice de calidad
                importancias = {}
                for var_name, var_data in variables_data.items():
                    if var_name != 'CO2_Impacto' and len(var_data.dropna()) > 1:
                        try:
                            # Usar correlación absoluta como medida de importancia
                            corr = abs(np.corrcoef(df['indice_calidad'].fillna(0.5), 
                                                  var_data.fillna(var_data.mean()))[0, 1])
                            if np.isnan(corr):
                                corr = 0.1  # Valor por defecto
                            importancias[var_name] = corr
                        except:
                            importancias[var_name] = 0.1
                
                # Normalizar importancias para que sumen 1
                total = sum(importancias.values())
                if total > 0:
                    importancias = {k: v/total for k, v in importancias.items()}
                else:
                    # Valores por defecto
                    importancias = {
                        'CO2_Impacto': 0.40,
                        'Temperatura': 0.25,
                        'Humedad': 0.20,
                        'Presión': 0.15
                    }
            else:
                # Valores por defecto para pocos datos
                importancias = {
                    'CO2_Impacto': 0.40,
                    'Temperatura': 0.25,
                    'Humedad': 0.20,
                    'Presión': 0.15
                }
            
            # Preparar datos para gráfico
            variables = list(importancias.keys())
            valores = [importancias[var] for var in variables]
            
            # Colores para las barras
            colores = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
            
            # Gráfico de barras horizontales
            y_pos = np.arange(len(variables))
            bars = ax.barh(y_pos, valores, color=colores, edgecolor='black', height=0.6)
            
            ax.set_yticks(y_pos)
            ax.set_yticklabels(variables, fontweight='bold')
            ax.set_xlabel('Importancia Relativa', fontweight='bold')
            ax.set_title(titulo, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x', zorder=0)
            
            # Agregar valores porcentuales
            for i, (bar, val) in enumerate(zip(bars, valores)):
                ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2.,
                       f'{val*100:.1f}%', va='center', fontweight='bold')
            
            # Limitar eje X a 100%
            ax.set_xlim([0, 1])
            
            # Agregar nota sobre cálculo
            if len(df) >= 5:
                ax.text(0.02, 0.02, f'Calculado de {len(df)} muestras', 
                       transform=ax.transAxes, fontsize=8, 
                       bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.5))
            
            return bars
            
        except Exception as e:
            print(f"[DEBUG] Error en gráfico de importancia: {e}")
            ax.text(0.5, 0.5, 'Error calculando importancias', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'{titulo} - Error')
            return None
    
    def crear_tabla_estadisticas(self, ax, df_estadisticas, df_ultima, rango):
        """Crea tabla con estadísticas - MODIFICADO"""
        if df_estadisticas.empty or df_ultima.empty:
            ax.text(0.5, 0.5, 'No hay datos estadísticos', 
                   ha='center', va='center', fontsize=12, color='red')
            ax.set_title(f'Estadísticas del Sistema - Sin datos')
            ax.set_facecolor('#f8f8f8')
            return None
        
        print(f"[DEBUG] Creando tabla de estadísticas")
        
        # Preparar datos para la tabla
        estadisticas = df_estadisticas.iloc[0]
        ultima = df_ultima.iloc[0]
        
        # Formatear fecha mínima y máxima
        fecha_min = "N/A"
        fecha_max = "N/A"
        if 'fecha_min' in estadisticas and estadisticas['fecha_min']:
            try:
                fecha_min = str(estadisticas['fecha_min'])[:10]
            except:
                fecha_min = str(estadisticas['fecha_min'])
        
        if 'fecha_max' in estadisticas and estadisticas['fecha_max']:
            try:
                fecha_max = str(estadisticas['fecha_max'])[:10]
            except:
                fecha_max = str(estadisticas['fecha_max'])
        
        # Formatear hora de última medición
        ultima_hora = "N/A"
        if 'created_at' in ultima and ultima['created_at']:
            try:
                if isinstance(ultima['created_at'], str):
                    if 'T' in ultima['created_at']:
                        fecha = ultima['created_at'].split('T')[0]
                        hora = ultima['created_at'].split('T')[1][:8]
                        ultima_hora = f"{fecha} {hora}"
                    else:
                        ultima_hora = ultima['created_at']
                else:
                    ultima_hora = str(ultima['created_at'])
            except:
                ultima_hora = "N/A"
        
        datos_tabla = [
            ['SISTEMA COMPLETO', ''],
            ['Total muestras', f"{int(estadisticas['total_muestras']):,}"],
            ['Temp promedio', f"{estadisticas['temp_promedio']:.1f}°C"],
            ['Hum promedio', f"{estadisticas['humedad_promedio']:.1f}%"],
            ['Pres promedio', f"{estadisticas['presion_promedio']:.1f} hPa"],
            ['Periodo', f"{fecha_min} a {fecha_max}"],
            ['', ''],
            ['ÚLTIMA MEDICIÓN', ''],
            ['Calidad', ultima['calidad_aire_pred']],
            ['CO2 nivel', ultima['co2_nivel']],
            ['Temperatura', f"{ultima['temperatura']:.1f}°C"],
            ['Humedad', f"{ultima['humedad']:.1f}%"],
            ['Fecha/Hora', ultima_hora]
        ]
        
        # Crear tabla
        tabla = ax.table(cellText=datos_tabla, 
                        cellLoc='left',
                        loc='center',
                        colWidths=[0.5, 0.5],
                        cellColours=[['#f0f0f0', '#f0f0f0']] * len(datos_tabla))
        
        # Formatear tabla
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(10)
        tabla.scale(1, 1.8)
        
        # Colorear filas de encabezado
        for i, key in enumerate(datos_tabla):
            if key[0] in ['SISTEMA COMPLETO', 'ÚLTIMA MEDICIÓN']:
                for j in range(len(key)):
                    tabla[(i, j)].set_facecolor('#2E86AB')
                    tabla[(i, j)].set_text_props(weight='bold', color='white')
            elif key[0] == '':
                for j in range(len(key)):
                    tabla[(i, j)].set_facecolor('#f8f8f8')
        
        # Colorear fila de calidad según categoría
        for i, key in enumerate(datos_tabla):
            if key[0] == 'Calidad':
                color_calidad = self.colores_calidad.get(key[1], '#808080')
                tabla[(i, 1)].set_facecolor(color_calidad)
                tabla[(i, 1)].set_text_props(weight='bold')
        
        ax.axis('off')
        ax.set_title('Estadísticas del Sistema', fontweight='bold', pad=20)
        
        # Agregar nota temporal
        ax.text(0.02, 0.02, f'Dashboard: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
               transform=ax.transAxes, fontsize=8, style='italic')
        
        return tabla
    
    def crear_dashboard_historico(self, registros=50, guardar_imagen=False):
        """Crea dashboard con datos históricos (por días/registros)"""
        try:
            print("\n" + "="*60)
            print(f"DASHBOARD HISTÓRICO - ÚLTIMOS {registros} REGISTROS")
            print("="*60)
            
            # 1. Obtener datos históricos
            df = self.obtener_ultimos_registros(registros)
            
            # 2. Obtener estadísticas de TODOS los datos
            df_categorias, df_estadisticas, df_ultima = self.obtener_estadisticas_completas()
            
            if df.empty:
                print("ERROR: No hay datos en la base de datos.")
                print("Ejecute primero 'Procesar datos de sensores (JSON)'")
                return None
            
            # 3. Crear figura
            self.fig, axs = plt.subplots(2, 2, figsize=(16, 12))
            
            # Título principal
            if not df.empty:
                fecha_min = df['timestamp'].min().strftime('%d/%m/%Y')
                fecha_max = df['timestamp'].max().strftime('%d/%m/%Y')
                rango_fechas = f"{fecha_min} a {fecha_max}"
            else:
                rango_fechas = "Sin datos"
            
            titulo_principal = f'DASHBOARD CALIDAD DEL AIRE - UPS CAMPUS CENTENARIO\n'
            
            self.fig.suptitle(titulo_principal, fontsize=16, fontweight='bold', y=0.98)
            self.fig.patch.set_facecolor('#f5f5f5')
            
            # Ajustar espaciado
            plt.subplots_adjust(hspace=0.35, wspace=0.3, top=0.92)
            
            # 4. Crear los 4 gráficos
            print(f"\n[PROCESO] Creando gráficos...")
            
            # Gráfico 1: Series de tiempo
            print(f"  -> Gráfico 1: Series de tiempo")
            titulo_series = f'Evolución Histórica de Calidad del Aire\n{registros} registros'
            scatter = self.crear_grafico_series_tiempo(axs[0, 0], df, titulo_series)
            
            # Gráfico 2: Distribución de categorías (de TODOS los datos)
            print(f"  -> Gráfico 2: Distribución de categorías")
            titulo_categorias = f'Distribución de Categorías (Todos los datos)'
            bars = self.crear_grafico_categorias(axs[0, 1], df_categorias, titulo_categorias)
            
            # Gráfico 3: Importancia de variables (de los datos mostrados)
            print(f"  -> Gráfico 3: Importancia de variables")
            titulo_importancia = f'Importancia de Variables en Calidad'
            importance_bars = self.crear_grafico_importancia(axs[1, 0], df, titulo_importancia)
            
            # Gráfico 4: Tabla de estadísticas
            print(f"  -> Gráfico 4: Tabla de estadísticas")
            tabla = self.crear_tabla_estadisticas(axs[1, 1], df_estadisticas, df_ultima, f"{registros} registros")
            
            # 5. Guardar imagen si se solicita
            if guardar_imagen:
                fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
                imagen_path = os.path.join(self.proyecto_root, 'reports', 
                                          f'dashboard_historico_{registros}_{fecha_actual}.png')
                os.makedirs(os.path.dirname(imagen_path), exist_ok=True)
                plt.savefig(imagen_path, dpi=150, bbox_inches='tight', facecolor='#f5f5f5')
                print(f"\n[ARCHIVO] Dashboard guardado como: {imagen_path}")
            
            # 6. Mostrar dashboard
            print(f"\n[MOSTRAR] Visualizando dashboard...")
            plt.tight_layout()
            plt.show()
            
            # 7. Mostrar resumen en consola
            self.mostrar_resumen_historico(df, df_estadisticas, df_ultima, registros)
            
            return self.fig
            
        except Exception as e:
            print(f"\n[ERROR] Error ejecutando dashboard histórico: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def mostrar_resumen_historico(self, df, df_estadisticas, df_ultima, registros):
        """Muestra resumen en consola para datos históricos"""
        print("\n" + "="*70)
        print(f"RESUMEN HISTÓRICO - ÚLTIMOS {registros} REGISTROS")
        print("="*70)
        
        if not df_estadisticas.empty:
            stats = df_estadisticas.iloc[0]
            print(f" ESTADÍSTICAS GENERALES DEL SISTEMA:")
            print(f"   * Total de muestras: {int(stats['total_muestras']):,}")
            print(f"   * Temperatura promedio: {stats['temp_promedio']:.1f}°C")
            print(f"   * Humedad promedio: {stats['humedad_promedio']:.1f}%")
            print(f"   * Presión promedio: {stats['presion_promedio']:.1f} hPa")
            
            if 'fecha_min' in stats and 'fecha_max' in stats:
                print(f"   * Periodo de datos: {stats['fecha_min'][:10]} a {stats['fecha_max'][:10]}")
        
        if not df.empty:
            print(f"\n DATOS MOSTRADOS ({registros} registros):")
            print(f"   * Muestras en gráfico: {len(df)}")
            
            if 'calidad_aire_pred' in df.columns and not df['calidad_aire_pred'].isna().all():
                distribucion = df['calidad_aire_pred'].value_counts()
                print(f"   * Distribución por categoría:")
                for cat, count in distribucion.items():
                    porcentaje = (count / len(df)) * 100 if len(df) > 0 else 0
                    print(f"     - {cat}: {count} muestras ({porcentaje:.1f}%)")
            
            if 'temperatura' in df.columns:
                print(f"   * Temperatura: {df['temperatura'].mean():.1f}°C "
                      f"(min: {df['temperatura'].min():.1f}°C, max: {df['temperatura'].max():.1f}°C)")
            
            # Mostrar rango de fechas
            fecha_min = df['timestamp'].min().strftime('%Y-%m-%d')
            fecha_max = df['timestamp'].max().strftime('%Y-%m-%d')
            print(f"   * Periodo mostrado: {fecha_min} a {fecha_max}")
        
        if not df_ultima.empty:
            ultima = df_ultima.iloc[0]
            print(f"\n ÚLTIMA MEDICIÓN REGISTRADA EN EL SISTEMA:")
            print(f"   * Calidad: {ultima['calidad_aire_pred']}")
            print(f"   * Nivel CO2: {ultima['co2_nivel']}")
            print(f"   * Temperatura: {ultima['temperatura']:.1f}°C")
            print(f"   * Humedad: {ultima['humedad']:.1f}%")
            
            if 'created_at' in ultima and ultima['created_at']:
                fecha_hora = str(ultima['created_at'])
                print(f"   * Fecha/Hora: {fecha_hora[:19]}")
        
        print("\n RECOMENDACIONES:")
        if not df.empty and 'calidad_aire_pred' in df.columns and len(df) > 0:
            # Calcular la calidad predominante
            calidad_predominante = df['calidad_aire_pred'].mode()
            if not calidad_predominante.empty:
                calidad = calidad_predominante.iloc[0]
                if calidad in ['Excelente', 'Buena']:
                    print("    Condiciones óptimas en el periodo analizado.")
                elif calidad == 'Moderada':
                    print("    Condiciones aceptables. Monitorear continuamente.")
                elif calidad == 'Deficiente':
                    print("    Se requiere atención. Revisar fuentes de contaminación.")
                elif calidad in ['Muy deficiente', 'Peligrosa']:
                    print("    ALERTA: Condiciones peligrosas detectadas en el historial.")
        
        print("="*70)
    
    def menu_interactivo(self):
        """Menu interactivo para el dashboard - MODIFICADO para datos históricos"""
        while True:
            print("\n" + "="*70)
            print("DASHBOARD INTERACTIVO - CALIDAD DEL AIRE")
            print("="*70)
            print("1. Ver dashboard completo")
            print("2. Ver dashboard con 100 registros")
            print("3. Ver dashboard con todos los registros")
            print("4. Ver dashboard personalizado")
            print("5. Guardar dashboard actual como imagen")
            print("6. Diagnóstico completo de base de datos")
            print("7. Volver al menú principal")
            print("="*70)
            
            opcion = input("Seleccione una opción (1-7): ").strip()
            
            if opcion == "1":
                self.crear_dashboard_historico(registros=50)
            elif opcion == "2":
                self.crear_dashboard_historico(registros=100)
            elif opcion == "3":
                # Obtener el total de registros disponibles
                with self.conectar_db() as conn:
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM sensor_responses")
                        total = cursor.fetchone()[0]
                        self.crear_dashboard_historico(registros=total)
            elif opcion == "4":
                try:
                    registros = int(input("Ingrese número de registros a mostrar: "))
                    if registros > 0:
                        self.crear_dashboard_historico(registros=registros)
                    else:
                        print("Error: Debe ingresar un número positivo")
                except ValueError:
                    print("Error: Ingrese un número válido")
            elif opcion == "5":
                self.crear_dashboard_historico(registros=50, guardar_imagen=True)
            elif opcion == "6":
                self.verificar_estructura_db()
            elif opcion == "7":
                print("Saliendo del dashboard...")
                break
            else:
                print("Opción inválida. Intente nuevamente.")
            
            if opcion in ["1", "2", "3", "4", "5", "6"]:
                input("\nPresione Enter para continuar...")

def main():
    """Función principal - MODIFICADA para datos históricos"""
    print("="*70)
    print(" DASHBOARD DE VISUALIZACIÓN - SISTEMA DE CALIDAD DEL AIRE")
    print("="*70)
    print("Versión: 4.0 - Modo Histórico")
    print("Desarrollado para: UPS Campus Centenario - Guayaquil")
    print("="*70)
    
    dashboard = DashboardCalidadAire()
    
    # Verificar si la base de datos existe
    if not os.path.exists(dashboard.db_path):
        print(f"\n ERROR: Base de datos no encontrada en:")
        print(f"   {dashboard.db_path}")
        print(f"\n ACCIONES REQUERIDAS:")
        print(f"   1. Ejecute 'Procesar datos de sensores (JSON)' desde Main.py")
        print(f"   2. Verifique que existan archivos JSON en data/raw_json/")
        print(f"   3. Revise la carpeta data/database/")
        print("="*70)
        return
    
    # Diagnóstico rápido
    print(f"\n Realizando diagnóstico rápido...")
    with dashboard.conectar_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sensor_responses")
        total_muestras = cursor.fetchone()[0]
        print(f"   * Muestras en base de datos: {total_muestras}")
        
        cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM sensor_responses")
        min_max = cursor.fetchone()
        if min_max[0] and min_max[1]:
            fecha_inicio = str(min_max[0])[:10]
            fecha_fin = str(min_max[1])[:10]
            print(f"   * Periodo de datos: {fecha_inicio} a {fecha_fin}")
        
        # Verificar nombre de columna de temperatura
        cursor.execute("PRAGMA table_info(sensor_responses)")
        columnas = cursor.fetchall()
        col_temp = None
        for col in columnas:
            if col[1] in ['temperature', 'temperatura']:
                col_temp = col[1]
                break
        print(f"   * Columna temperatura: {col_temp}")
    
    if total_muestras == 0:
        print(f"\n  ADVERTENCIA: Base de datos vacía")
        print(f"   Ejecute 'Procesar datos de sensores (JSON)' primero")
    
    print("="*70)
    
    # Menú interactivo
    dashboard.menu_interactivo()

if __name__ == "__main__":
    main()