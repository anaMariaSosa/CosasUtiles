import pandas as pd

# Preguntar al usuario la ruta del archivo
file_path = input("👉 Introduce la ruta del archivo CSV o Excel: ").strip()

try:
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Formato no reconocido. Usa .csv o .xlsx")

    print(f"\n✅ Archivo cargado correctamente: {file_path}")
    display(df.head())

except FileNotFoundError:
    print("❌ No se encontró el archivo. Verifica la ruta.")
except Exception as e:
    print(f"⚠️ Ocurrió un error: {e}")
