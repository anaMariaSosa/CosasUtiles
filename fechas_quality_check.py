import re
import pandas as pd

def check_date(
    df: pd.DataFrame,
    col_bool_name: str,
    col_date_name: str,
    cutoff_year: int = 2000,
    cutoff_month: int = 2,
    cutoff_day: int = 1
) -> pd.DataFrame:
    s = df[col_date_name]

    # 1) Normalizamos strings obvios (quita espacios, unifica separadores)
    s_norm = s.astype(str).str.strip().str.replace(r'[-.]', '/', regex=True)

    # 2) Fechas Excel numéricas -> Timestamp (mantén valores no numéricos como NaN)
    #    Solo intentamos donde es numérico puro
    is_numeric = s_norm.str.fullmatch(r'\d+(\.\d+)?', na=False)
    dt_num = pd.to_datetime(
        pd.to_numeric(s_norm.where(is_numeric), errors='coerce'),
        unit='d', origin='1899-12-30', errors='coerce'
    )

    # 3) Intento general con dayfirst=True (cubre DD/MM/YYYY y con hora)
    dt1 = pd.to_datetime(s_norm, errors='coerce', dayfirst=True, infer_datetime_format=True)

    # 4) Relleno lo que siga NaT probando monthfirst (MM/DD/YYYY) / yearfirst
    missing = dt1.isna()
    dt2 = pd.to_datetime(s_norm.where(missing), errors='coerce', dayfirst=False, infer_datetime_format=True)

    # 5) Casos raros tipo "YYYY/DD/MM": si el segundo campo > 12, intercambiamos DD y MM
    #    Reordenamos a YYYY/MM/DD para poder parsear.
    def fix_yyyy_dd_mm(x: str) -> str:
        m = re.fullmatch(r'(\d{4})/(\d{1,2})/(\d{1,2})', x or '')
        if not m:
            return x
        y, a, b = m.groups()
        # si la 2ª parte no puede ser mes pero la 3ª sí, asumimos YYYY/DD/MM -> YYYY/MM/DD
        if a.isdigit() and b.isdigit():
            ai, bi = int(a), int(b)
            if ai > 12 and 1 <= bi <= 12:
                return f"{y}/{b.zfill(2)}/{a.zfill(2)}"
        return x

    s_fixed = s_norm.where(~missing, s_norm.where(missing).map(fix_yyyy_dd_mm))
    dt3 = pd.to_datetime(s_fixed.where(missing), errors='coerce', yearfirst=True, infer_datetime_format=True)

    # 6) Componemos la serie final priorizando: numérico -> dt1 -> dt2 -> dt3
    dt = dt_num.combine_first(dt1).combine_first(dt2).combine_first(dt3)

    # 7) Definimos los cortes
    cutoff = pd.Timestamp(cutoff_year, cutoff_month, cutoff_day)
    now = pd.Timestamp.now()  # naive; dt también es naive

    # 8) Condición: TRUE si fecha < 01/02/2000 **o** fecha > ahora; FALSE en el resto.
    #    NaT debe dar FALSE por definición de validación (ajústalo si quieres tratar NaT aparte).
    condition = (dt < cutoff) | (dt > now)
    condition = condition.fillna(False)

    df[col_bool_name] = condition
    return df
