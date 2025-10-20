########### eliminar filas de rdcd

condicion = (
    df["descripcion"].str.contains("matiz", case=False, na=False) &
    (
        df["descripcion"].str.contains("hola", case=False, na=False) |
        df["descripcion"].str.contains("adios", case=False, na=False)
    )
)

# --- Filtrar manteniendo las demás ---
df_filtrado = df[~condicion]

########### fechas para ok_M
import pandas as pd
import numpy as np
import re

def _parse_mixed_datetime(s: object) -> pd.Timestamp:
    """
    Intenta parsear una fecha/hora que puede venir en D-M-Y o M-D-Y (con o sin hora).
    Heurística:
      - Si el primer número > 12 -> interpretamos como D-M-Y
      - Si el segundo número > 12 -> interpretamos como M-D-Y
      - Si ambos <= 12 (ambiguo) -> probamos D-M-Y y luego M-D-Y
    """
    if pd.isna(s):
        return pd.NaT
    txt = str(s).strip()
    if not txt:
        return pd.NaT

    # Tomamos los dos primeros tokens numéricos para decidir dayfirst
    nums = re.findall(r'\d+', txt)
    dayfirst_guess = None
    if len(nums) >= 2:
        a = int(nums[0])
        b = int(nums[1])
        if a > 12:
            dayfirst_guess = True   # 31-01-2025 -> D-M-Y
        elif b > 12:
            dayfirst_guess = False  # 01-31-2025 -> M-D-Y

    # Intento principal
    if dayfirst_guess is not None:
        dt = pd.to_datetime(txt, errors="coerce", dayfirst=dayfirst_guess, utc=False)
        if pd.notna(dt):
            return dt

        # Fallback: probar la otra interpretación
        dt = pd.to_datetime(txt, errors="coerce", dayfirst=not dayfirst_guess, utc=False)
        return dt

    # Ambiguo: probar D-M-Y y luego M-D-Y
    dt = pd.to_datetime(txt, errors="coerce", dayfirst=True, utc=False)
    if pd.notna(dt):
        return dt
    dt = pd.to_datetime(txt, errors="coerce", dayfirst=False, utc=False)
    return dt


def comparar_fechas_mixtas(df: pd.DataFrame, col1: str, col2: str, out_col: str = "resultado") -> pd.DataFrame:
    """
    Compara dos columnas de fechas/hora con formato mixto (D-M-Y o M-D-Y).
    Escribe 'ok' si son iguales (misma marca de tiempo), 'revisar' si no.
    - Si alguna no se puede parsear -> 'revisar'.
    - La comparación es exacta (incluye hora, minutos, segundos si existen).
    """
    d1 = df[col1].map(_parse_mixed_datetime)
    d2 = df[col2].map(_parse_mixed_datetime)

    iguales = (d1.notna() & d2.notna() & (d1 == d2))
    df[out_col] = np.where(iguales, "ok", "revisar")
    return df


############ uso:

df = comparar_fechas_mixtas(df, "fecha_a", "fecha_b", out_col="resultado")