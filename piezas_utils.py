from typing import List, Iterable, Tuple, Optional, Union
import pandas as pd

def _ensure_datetime(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Internal helper to ensure the date column is datetime (no timezone)."""
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.tz_localize(None)
    return out

def _presence_mask(keys: pd.DataFrame, other: pd.DataFrame, clave_cols: List[str]) -> pd.Series:
    other_keys = other[clave_cols].drop_duplicates()
    return keys.merge(other_keys.assign(__flag=True), on=clave_cols, how="left")["__flag"].fillna(False)

def max_fecha_por_pieza(
    df: pd.DataFrame,
    pieza_col: str,
    fecha_col: str,
    keep_rows: bool = True,
) -> pd.DataFrame:
    """
    1) Dado un fichero (DataFrame) de movimientos con varias fechas por pieza,
       devuelve por pieza la FECHA MÁS GRANDE.
    
    Parámetros
    ----------
    df : DataFrame con movimientos.
    pieza_col : Nombre de la columna clave de pieza (str).
    fecha_col : Nombre de la columna de fecha (str).
    keep_rows : 
        - True  -> devuelve las FILAS completas del último movimiento por pieza.
        - False -> devuelve un DataFrame con columnas [pieza_col, fecha_col_max].
    
    Retorna
    -------
    DataFrame
    """
    dft = _ensure_datetime(df, fecha_col)
    idx = dft.groupby(pieza_col)[fecha_col].idxmax()
    if keep_rows:
        return dft.loc[idx].sort_values(by=[pieza_col]).reset_index(drop=True)
    else:
        out = (
            dft.groupby(pieza_col, as_index=False)[fecha_col]
            .max()
            .rename(columns={fecha_col: f"{fecha_col}_max"})
            .sort_values(by=[pieza_col])
            .reset_index(drop=True)
        )
        return out


def min_fecha_por_pieza(
    df: pd.DataFrame,
    pieza_col: str,
    fecha_col: str,
    keep_rows: bool = True,
) -> pd.DataFrame:
    """
    2) Dado un fichero (DataFrame) de movimientos con varias fechas por pieza,
       devuelve por pieza la FECHA MÁS PEQUEÑA.
    
    Parámetros
    ----------
    df : DataFrame con movimientos.
    pieza_col : Nombre de la columna clave de pieza (str).
    fecha_col : Nombre de la columna de fecha (str).
    keep_rows : 
        - True  -> devuelve las FILAS completas del primer movimiento por pieza.
        - False -> devuelve un DataFrame con columnas [pieza_col, fecha_col_min].
    
    Retorna
    -------
    DataFrame
    """
    dft = _ensure_datetime(df, fecha_col)
    idx = dft.groupby(pieza_col)[fecha_col].idxmin()
    if keep_rows:
        return dft.loc[idx].sort_values(by=[pieza_col]).reset_index(drop=True)
    else:
        out = (
            dft.groupby(pieza_col, as_index=False)[fecha_col]
            .min()
            .rename(columns={fecha_col: f"{fecha_col}_min"})
            .sort_values(by=[pieza_col])
            .reset_index(drop=True)
        )
        return out


def comparar_por_clave(
    f1: pd.DataFrame,
    f2: pd.DataFrame,
    clave_cols: Union[str, List[str]],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    3) Compara dos ficheros (DataFrames) por clave para obtener:
       - Sólo en f1
       - Sólo en f2
       - En ambos
    
    Parámetros
    ----------
    f1, f2 : DataFrames a comparar.
    clave_cols : columna o lista de columnas clave.
    
    Retorna
    -------
    (solo_f1, solo_f2, en_ambos) como DataFrames con las columnas clave.
    """
    if isinstance(clave_cols, str):
        clave_cols = [clave_cols]

    # Usamos drop_duplicates para claves únicas
    k1 = f1[clave_cols].drop_duplicates().assign(__src="f1")
    k2 = f2[clave_cols].drop_duplicates().assign(__src="f2")

    merged = k1.merge(k2, on=clave_cols, how="outer", indicator=True)

    solo_f1 = merged[merged["_merge"] == "left_only"][clave_cols].reset_index(drop=True)
    solo_f2 = merged[merged["_merge"] == "right_only"][clave_cols].reset_index(drop=True)
    en_ambos = merged[merged["_merge"] == "both"][clave_cols].reset_index(drop=True)

    return solo_f1, solo_f2, en_ambos


def chequear_primer_movimiento_no_valor(
    f1: pd.DataFrame,
    pieza_col: str,
    fecha_col: str,
    valor_col: str,
    valores_prohibidos: Iterable,
    f2: Optional[pd.DataFrame] = None,
    f3: Optional[pd.DataFrame] = None,
    clave_cols_f2: Optional[Union[str, List[str]]] = None,
    clave_cols_f3: Optional[Union[str, List[str]]] = None,
    condicion_en_f2: Optional[bool] = None,
    condicion_en_f3: Optional[bool] = None,
) -> pd.DataFrame:
    """
    4) Chequea que el PRIMER movimiento en f1 NO puede ser un valor (X o Y)
       si se cumplen condiciones de presencia (está / no está) en f2 y/o f3.
    
    La regla se aplica sólo cuando condicion_en_f2 / condicion_en_f3 no es None.
    Si ambos son None, la regla se aplica SIN mirar f2/f3 (sólo valores prohibidos).
    
    Parámetros
    ----------
    f1 : DataFrame de movimientos con varias filas por pieza.
    pieza_col : nombre de la columna clave de pieza en f1.
    fecha_col : nombre de la columna de fecha en f1.
    valor_col : columna cuyo primer valor por pieza queremos validar.
    valores_prohibidos : iterable con valores (por ejemplo {"X", "Y"}).
    f2, f3 : DataFrames auxiliares para condiciones de presencia (opcionales).
    clave_cols_f2, clave_cols_f3 : columna(s) clave para cruzar con f2/f3.
        - Por defecto: pieza_col.
    condicion_en_f2 : 
        - True  -> la regla aplica si la pieza ESTÁ en f2.
        - False -> la regla aplica si la pieza NO ESTÁ en f2.
        - None  -> ignora f2.
    condicion_en_f3 : análogo a condicion_en_f2.
    
    Retorna
    -------
    DataFrame con las piezas que INCUMPLEN la regla. Incluye:
    [pieza_col, fecha_primera, valor_primero, aplica_regla_f2, aplica_regla_f3].
    """
    dft = _ensure_datetime(f1, fecha_col)

    # Primer movimiento por pieza (fecha mínima)
    first_rows = dft.loc[dft.groupby(pieza_col)[fecha_col].idxmin(), [pieza_col, fecha_col, valor_col]].copy()
    first_rows.rename(columns={fecha_col: "fecha_primera", valor_col: "valor_primero"}, inplace=True)

    first_rows["aplica_regla_f2"] = True
    first_rows["aplica_regla_f3"] = True

    # Determinar si debe aplicarse la regla según presencia en f2/f3
    if condicion_en_f2 is not None and f2 is not None:
        ccols2 = [pieza_col] if clave_cols_f2 is None else ([clave_cols_f2] if isinstance(clave_cols_f2, str) else clave_cols_f2)
        present_in_f2 = _presence_mask(first_rows[[pieza_col]], f2, ccols2).values
        first_rows["aplica_regla_f2"] = (present_in_f2 == condicion_en_f2)
    elif condicion_en_f2 is not None:
        # Se pidió condicionar por f2 pero f2 no se proporcionó
        first_rows["aplica_regla_f2"] = False  # no aplica

    if condicion_en_f3 is not None and f3 is not None:
        ccols3 = [pieza_col] if clave_cols_f3 is None else ([clave_cols_f3] if isinstance(clave_cols_f3, str) else clave_cols_f3)
        present_in_f3 = _presence_mask(first_rows[[pieza_col]], f3, ccols3).values
        first_rows["aplica_regla_f3"] = (present_in_f3 == condicion_en_f3)
    elif condicion_en_f3 is not None:
        first_rows["aplica_regla_f3"] = False

    # La regla aplica si (para cada fuente especificada) coincide la condición
    masks = []
    if condicion_en_f2 is not None:
        masks.append(first_rows["aplica_regla_f2"])
    if condicion_en_f3 is not None:
        masks.append(first_rows["aplica_regla_f3"])
    aplica_regla = pd.Series(True, index=first_rows.index) if not masks else masks[0]
    for m in masks[1:]:
        aplica_regla = aplica_regla & m

    # Violaciones: aplica_regla y valor_primero es prohibido
    violaciones = first_rows[aplica_regla & first_rows["valor_primero"].isin(set(valores_prohibidos))].copy()
    violaciones.sort_values(by=[pieza_col], inplace=True)
    violaciones.reset_index(drop=True, inplace=True)
    return violaciones


def piezas_por_presencia(
    f1: pd.DataFrame,
    f2: pd.DataFrame,
    f3: pd.DataFrame,
    pieza_col: str,
    en_f1: bool = True,
    en_f2: bool = True,
    en_f3: bool = True,
) -> pd.DataFrame:
    """
    5) Devuelve las piezas que cumplen una lógica de presencia/ausencia
       simultánea en f1, f2 y f3.
    
    Parámetros
    ----------
    f1, f2, f3 : DataFrames de entrada.
    pieza_col : nombre de la columna clave (debe existir en los 3 DataFrames).
    en_f1, en_f2, en_f3 : booleans que indican si la pieza debe ESTAR (True)
                          o NO ESTAR (False) en cada fichero.
    
    Retorna
    -------
    DataFrame con una única columna [pieza_col] con las piezas que cumplen.
    """
    keys = pd.DataFrame({pieza_col: pd.concat([f1[pieza_col], f2[pieza_col], f3[pieza_col]]).drop_duplicates()})
    def present(df):
        return keys.merge(df[[pieza_col]].drop_duplicates().assign(__p=True), on=pieza_col, how="left")["__p"].fillna(False)

    mask = pd.Series(True, index=keys.index)

    for df, required in [(f1, en_f1), (f2, en_f2), (f3, en_f3)]:
        m = present(df)
        mask = mask & (m if required else ~m)

    return keys.loc[mask, [pieza_col]].reset_index(drop=True)


def concatenar_por_clave(
    base: pd.DataFrame,
    ficheros: List[pd.DataFrame],
    clave_cols: Union[str, List[str]],
    sufijos: Optional[List[str]] = None,
    how: str = "left"
) -> pd.DataFrame:
    """
    6) Concatena (mergea) columnas de varios ficheros (DataFrames)
       en base a una o varias claves comunes.

    Ejemplo típico:
        en_ambos = comparar_por_clave(f1, f2, "pieza")[2]
        resultado = concatenar_por_clave(en_ambos, [f1, f2], "pieza", sufijos=["_f1", "_f2"])

    Parámetros
    ----------
    base : DataFrame base, por ejemplo el resultado de 'en_ambos'.
    ficheros : lista de DataFrames (f1, f2, f3...) que contienen las columnas a unir.
    clave_cols : nombre o lista de nombres de columnas clave.
    sufijos : lista de sufijos a aplicar a las columnas de cada fichero.
              Si no se indica, se usarán automáticamente "_1", "_2", "_3", etc.
    how : tipo de merge con pandas (por defecto 'left').

    Retorna
    -------
    DataFrame con las claves + columnas concatenadas de todos los ficheros.
    """
    if isinstance(clave_cols, str):
        clave_cols = [clave_cols]

    result = base.copy()
    n = len(ficheros)
    if sufijos is None:
        sufijos = [f"_{i+1}" for i in range(n)]

    for i, (df, suf) in enumerate(zip(ficheros, sufijos)):
        cols_to_add = [c for c in df.columns if c not in clave_cols]
        renamed = df[clave_cols + cols_to_add].rename(
            columns={c: f"{c}{suf}" for c in cols_to_add}
        )
        result = result.merge(renamed, on=clave_cols, how=how)

    return result
