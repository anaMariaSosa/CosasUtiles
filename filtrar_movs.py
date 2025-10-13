import pandas as pd

def filtra_ultimo_movimiento(
    df: pd.DataFrame,
    claves: list[str],
    col_fecha: str = "fecha_movimiento"
) -> pd.DataFrame:
    """
    Devuelve un DataFrame con solo la fila más reciente (mayor fecha)
    para cada combinación de 'claves' (por ejemplo ['equipo'] o ['equipo', 'componente']).

    Parámetros:
    - df: DataFrame de entrada.
    - claves: lista de columnas que definen el grupo (ej. ['equipo', 'componente']).
    - col_fecha: columna con la fecha a usar como criterio.

    Retorna:
    - DataFrame filtrado con las filas más recientes por grupo.
    """
    df = df.copy()

    # Convertir la columna de fecha a datetime por seguridad
    df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")

    # Ordenar por claves y fecha
    df = df.sort_values(claves + [col_fecha])

    # Eliminar duplicados quedándote con el último (fecha más grande)
    df_ultimo = df.drop_duplicates(subset=claves, keep="last")

    return df_ultimo


#modo de uso
data = {
    "equipo": ["E1", "E1", "E2", "E2", "E2"],
    "componente": ["C1", "C1", "C2", "C2", "C3"],
    "movimiento": ["INST", "RET", "INST", "REV", "RET"],
    "fecha_movimiento": [
        "2024-01-01", "2024-03-01", "2023-05-01", "2023-07-01", "2023-02-01"
    ]
}

df = pd.DataFrame(data)

df_filtrado = filtra_ultimo_movimiento(df, claves=["equipo", "componente"], col_fecha="fecha_movimiento")
print(df_filtrado)