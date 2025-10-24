import pandas as pd
import os
import re
from pathlib import Path

# Carpetas de materias a procesar
materias = ['Álgebra', 'Algoritmos y Estructuras de Datos', 'Probabilidad y Estadística']

# Carpeta de salida para archivos limpios
output_folder = 'datos_limpios'
os.makedirs(output_folder, exist_ok=True)

def limpiar_calificacion(texto):
    """
    Limpia el campo de calificación.
    Si tiene &#número;, lo elimina y se queda con lo que sigue después del ;
    """
    if pd.isna(texto):
        return texto
    
    # Buscar patrón &#número;
    match = re.search(r'&#\d+;\s*(.+)', str(texto))
    if match:
        return match.group(1).strip()
    return str(texto).strip()

def extraer_metadatos(df):
    """
    Extrae los metadatos de las primeras 4 filas
    """
    metadatos = {}
    
    for i in range(4):
        celda = str(df.iloc[i, 0])
        if ':' in celda:
            partes = celda.split(':', 1)
            clave = partes[0].strip()
            valor = partes[1].strip()
            
            # Limpiar la calificación si es necesario
            if 'Calificación' in clave:
                valor = limpiar_calificacion(valor)
            
            metadatos[clave] = valor
    
    return metadatos

def procesar_archivo(archivo_path, materia):
    """
    Procesa un archivo Excel de calificaciones
    """
    print(f"Procesando: {archivo_path}")
    
    # Leer todo el archivo sin header
    df_completo = pd.read_excel(archivo_path, header=None)
    
    # Extraer metadatos de las primeras 4 filas
    metadatos = extraer_metadatos(df_completo)
    
    # Leer el archivo con los datos reales (saltando las primeras 6 filas)
    # Fila 6 (índice 6) tiene los headers
    df_limpio = pd.read_excel(archivo_path, header=6)
    
    # Agregar columnas de metadatos
    df_limpio['Materia'] = materia
    df_limpio['Fecha_exportacion'] = metadatos.get('Fecha de exportación', '')
    df_limpio['Nombre_aula'] = metadatos.get('Nombre del aula', '')
    df_limpio['Calificacion'] = metadatos.get('Calificación', '')
    df_limpio['Responsable'] = metadatos.get('Responsable', '')
    
    return df_limpio

# Procesar todos los archivos
todos_los_datos = []

for materia in materias:
    carpeta_materia = Path(materia)
    
    if not carpeta_materia.exists():
        print(f"⚠️  Carpeta no encontrada: {materia}")
        continue
    
    # Buscar todos los archivos que empiezan con calificaciones_alumnos
    archivos_calificaciones = list(carpeta_materia.glob('calificaciones_alumnos*.xlsx'))
    
    print(f"\n📚 Procesando {materia}: {len(archivos_calificaciones)} archivos")
    
    for archivo in archivos_calificaciones:
        try:
            df = procesar_archivo(archivo, materia)
            todos_los_datos.append(df)
            print(f"   ✅ {archivo.name} - {len(df)} registros")
        except Exception as e:
            print(f"   ❌ Error en {archivo.name}: {e}")

# Combinar todos los DataFrames
if todos_los_datos:
    df_final = pd.concat(todos_los_datos, ignore_index=True)
    
    # Guardar el archivo consolidado
    archivo_salida = os.path.join(output_folder, 'calificaciones_consolidadas.xlsx')
    df_final.to_excel(archivo_salida, index=False)
    
    print(f"\n✨ Proceso completado!")
    print(f"📊 Total de registros: {len(df_final)}")
    print(f"💾 Archivo guardado en: {archivo_salida}")
    
    # Mostrar resumen
    print("\n📈 Resumen por materia:")
    print(df_final.groupby('Materia').size())
    
    print("\n📝 Primeras filas del resultado:")
    print(df_final.head())
else:
    print("\n⚠️  No se procesaron archivos")
