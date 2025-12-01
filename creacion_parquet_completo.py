import glob
import subprocess
import time
import sys
import os

# 1. Buscar todos los archivos JSON
archivos = glob.glob("*.json")
total = len(archivos)

print(f"--- INICIANDO PROCESO MAESTRO PARA {total} ARCHIVOS ---")
print(f"Usando Python en: {sys.executable}")

for i, archivo in enumerate(archivos):
    print(f"[{i+1}/{total}] Procesando: {archivo} ...")
    
    # 2. Llamar al script esclavo como un subproceso independiente
    # Esto asegura que la RAM se libere al 100% al terminar cada archivo
    resultado = subprocess.run(
        [sys.executable, "creacion_individual_completa.py", archivo],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    # 3. Verificar resultado
    if "EXITO" in resultado.stdout:
        print("   -> [OK] Guardado correctamente.")
    else:
        print(f"   -> [FALLO] No se pudo procesar {archivo}")
        print("   --- ERROR DETALLADO ---")
        print(resultado.stderr) # Aquí saldrá el error de Spark si falla
        print("   -----------------------")
    
    # Pequeña pausa para no saturar
    time.sleep(1)

print("--- TODO TERMINADO ---")
print("Tus datos están en la carpeta 'vgc_data_parquet_completo'")