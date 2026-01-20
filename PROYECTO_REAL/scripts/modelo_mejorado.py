import os
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import json
from datetime import datetime, timedelta

class ModeloCalidadAire:
    def __init__(self, modelo_path='models/modelo_calidad_aire.pkl'):
        self.proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.modelo_path = os.path.join(self.proyecto_root, modelo_path)
        self.scaler_path = os.path.join(self.proyecto_root, 'models/scaler.pkl')
        self.modelo = None
        self.scaler = StandardScaler()
        self.feature_names = None
        
    def generar_datos_entrenamiento(self, n_samples=1000):
        """Genera datos de entrenamiento realistas basados en patrones conocidos"""
        print("Generando datos de entrenamiento realistas...")
        
        np.random.seed(42)
        
        # Patrones diurnos (mas contaminacion en horas pico)
        horas = np.random.randint(0, 24, n_samples)
        # Mañana (7-9) y tarde (17-19) tienen mas contaminacion
        hora_factor = np.where(
            ((horas >= 7) & (horas <= 9)) | ((horas >= 17) & (horas <= 19)),
            np.random.uniform(1.5, 2.5, n_samples),
            np.random.uniform(0.8, 1.2, n_samples)
        )
        
        # Dias de semana vs fin de semana
        dia_semana = np.random.randint(0, 7, n_samples)
        dia_factor = np.where(dia_semana < 5,  # Lunes-Viernes
                             np.random.uniform(1.2, 1.8, n_samples),
                             np.random.uniform(0.7, 1.3, n_samples))  # Fin de semana
        
        # Generar caracteristicas base
        datos = {
            'co2': np.random.uniform(350, 1500, n_samples) * hora_factor * dia_factor,
            'temperatura': np.random.uniform(20, 35, n_samples),
            'humedad': np.random.uniform(40, 85, n_samples),
            'presion': np.random.uniform(1010, 1020, n_samples),
            'hora_dia': horas,
            'dia_semana': dia_semana,
            'mes': np.random.randint(1, 13, n_samples)
        }
        
        df = pd.DataFrame(datos)
        
        # Calcular calidad del aire objetivo (indice AQI simplificado)
        # Formula simplificada basada en CO2, temperatura y humedad
        df['calidad_objetivo'] = (
            df['co2'] * 0.6 +  # CO2 es el factor principal
            (df['temperatura'] - 25).abs() * 10 +  # Temperatura ideal ~25°C
            (df['humedad'] - 60).abs() * 2  # Humedad ideal ~60%
        ) / 100
        
        # Normalizar objetivo entre 0 y 1
        df['calidad_objetivo'] = (df['calidad_objetivo'] - df['calidad_objetivo'].min()) / \
                                 (df['calidad_objetivo'].max() - df['calidad_objetivo'].min())
        
        print(f"Datos generados: {n_samples} muestras")
        return df
    
    def entrenar_modelo(self, guardar=True):
        """Entrena el modelo Random Forest con datos realistas"""
        print("Entrenando modelo Random Forest...")
        
        # Generar datos de entrenamiento
        df = self.generar_datos_entrenamiento(1000)
        
        # Separar caracteristicas y objetivo
        self.feature_names = ['co2', 'temperatura', 'humedad', 'presion', 'hora_dia', 'dia_semana']
        X = df[self.feature_names]
        y = df['calidad_objetivo']
        
        # Dividir datos
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Escalar caracteristicas
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Crear y entrenar modelo
        self.modelo = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        
        self.modelo.fit(X_train_scaled, y_train)
        
        # Evaluar modelo
        train_score = self.modelo.score(X_train_scaled, y_train)
        test_score = self.modelo.score(X_test_scaled, y_test)
        
        print(f"Score entrenamiento: {train_score:.4f}")
        print(f"Score prueba: {test_score:.4f}")
        
        # Calcular importancia de caracteristicas
        importancias = dict(zip(self.feature_names, self.modelo.feature_importances_))
        print("\nImportancia de caracteristicas:")
        for feature, importancia in sorted(importancias.items(), key=lambda x: x[1], reverse=True):
            print(f"  {feature}: {importancia:.4f}")
        
        # Guardar modelo y scaler
        if guardar:
            os.makedirs(os.path.dirname(self.modelo_path), exist_ok=True)
            joblib.dump(self.modelo, self.modelo_path)
            joblib.dump(self.scaler, self.scaler_path)
            print(f"\nModelo guardado en: {self.modelo_path}")
            print(f"Scaler guardado en: {self.scaler_path}")
            
            # Guardar metadata del modelo
            metadata = {
                'feature_names': self.feature_names,
                'fecha_entrenamiento': datetime.now().isoformat(),
                'train_score': float(train_score),
                'test_score': float(test_score),
                'parametros': {
                    'n_estimators': 100,
                    'max_depth': 15,
                    'min_samples_split': 5,
                    'min_samples_leaf': 2
                }
            }
            
            metadata_path = os.path.join(self.proyecto_root, 'models/metadata_modelo.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"Metadata guardada en: {metadata_path}")
        
        return self.modelo
    
    def cargar_modelo(self):
        """Carga el modelo entrenado desde disco"""
        if os.path.exists(self.modelo_path) and os.path.exists(self.scaler_path):
            print("Cargando modelo pre-entrenado...")
            self.modelo = joblib.load(self.modelo_path)
            self.scaler = joblib.load(self.scaler_path)
            
            # Cargar feature names desde metadata
            metadata_path = os.path.join(self.proyecto_root, 'models/metadata_modelo.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    self.feature_names = metadata.get('feature_names', [])
            else:
                self.feature_names = ['co2', 'temperatura', 'humedad', 'presion', 'hora_dia', 'dia_semana']
            
            print("Modelo cargado exitosamente")
            return True
        else:
            print("Modelo no encontrado. Es necesario entrenar primero.")
            return False
    
    def predecir(self, features):
        """Realiza prediccion con el modelo"""
        if self.modelo is None:
            if not self.cargar_modelo():
                print("Entrenando nuevo modelo...")
                self.entrenar_modelo()
        
        # Convertir features a DataFrame manteniendo orden
        if isinstance(features, dict):
            # Asegurar que todas las caracteristicas necesarias esten presentes
            for feature in self.feature_names:
                if feature not in features:
                    features[feature] = 0  # Valor por defecto
            
            # Crear DataFrame en el orden correcto
            df = pd.DataFrame([features])[self.feature_names]
        else:
            df = pd.DataFrame([features])
        
        # Escalar caracteristicas
        X_scaled = self.scaler.transform(df)
        
        # Realizar prediccion
        prediccion = self.modelo.predict(X_scaled)[0]
        
        # Convertir prediccion a categoria de calidad del aire
        categoria = self._clasificar_prediccion(prediccion)
        
        # Calcular importancia de caracteristicas para esta prediccion
        importancias = dict(zip(self.feature_names, self.modelo.feature_importances_))
        
        return {
            'valor_prediccion': float(prediccion),
            'categoria': categoria,
            'importancia_caracteristicas': importancias,
            'caracteristicas_utilizadas': self.feature_names
        }
    
    def _clasificar_prediccion(self, valor):
        """Convierte valor numerico a categoria de calidad del aire"""
        if valor < 0.2:
            return "Excelente"
        elif valor < 0.4:
            return "Buena"
        elif valor < 0.6:
            return "Moderada"
        elif valor < 0.8:
            return "Deficiente"
        else:
            return "Peligrosa"
    
    def probar_prediccion(self):
        """Prueba el modelo con datos de ejemplo"""
        print("\nProbando prediccion con datos de ejemplo...")
        
        # Ejemplo 1: Condiciones optimas
        ejemplo_optimo = {
            'co2': 450,
            'temperatura': 24,
            'humedad': 60,
            'presion': 1013,
            'hora_dia': 10,
            'dia_semana': 2  # Martes
        }
        
        # Ejemplo 2: Condiciones criticas
        ejemplo_critico = {
            'co2': 1200,
            'temperatura': 32,
            'humedad': 85,
            'presion': 1010,
            'hora_dia': 18,
            'dia_semana': 1  # Lunes
        }
        
        print("Ejemplo 1 - Condiciones optimas:")
        print(f"  CO2: {ejemplo_optimo['co2']} ppm, Temp: {ejemplo_optimo['temperatura']}°C")
        resultado1 = self.predecir(ejemplo_optimo)
        print(f"  Prediccion: {resultado1['categoria']} (valor: {resultado1['valor_prediccion']:.3f})")
        
        print("\nEjemplo 2 - Condiciones criticas:")
        print(f"  CO2: {ejemplo_critico['co2']} ppm, Temp: {ejemplo_critico['temperatura']}°C")
        resultado2 = self.predecir(ejemplo_critico)
        print(f"  Prediccion: {resultado2['categoria']} (valor: {resultado2['valor_prediccion']:.3f})")

def main():
    """Funcion principal para entrenar/probar el modelo"""
    print("MODELO MEJORADO DE CALIDAD DEL AIRE")
    print("="*50)
    
    modelo = ModeloCalidadAire()
    
    # Verificar si existe modelo pre-entrenado
    if os.path.exists(modelo.modelo_path):
        print("Modelo pre-entrenado encontrado.")
        accion = input("¿Desea re-entrenar el modelo? (s/n): ").strip().lower()
        
        if accion == 's':
            modelo.entrenar_modelo()
        else:
            modelo.cargar_modelo()
    else:
        print("Modelo no encontrado. Entrenando nuevo modelo...")
        modelo.entrenar_modelo()
    
    # Probar con ejemplos
    modelo.probar_prediccion()
    
    print("\n" + "="*50)
    print("Modelo listo para usar en procesador_json.py")

if __name__ == "__main__":
    main()