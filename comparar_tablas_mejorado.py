import pandas as pd
import numpy as np
from typing import Iterable, List, Optional

def comparar_tablas(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    keys: List[str],
    include_left: Optional[Iterable[str]] = None,
    include_right: Optional[Iterable[str]] = None,
    left_name: str = "left",
    right_name: str = "right",
    # ¿qué columnas comparar para decidir OK/DISCREPANCIA?
    compare_on: Optional[Iterable[str]] = None,  # por nombre base (sin sufijo). Si None => intersección de incluidas
    # normalización de texto antes de comparar
    normalize_text_on: Optional[Iterable[str]] = None,  # por nombre base
    strip: bool = True,
    lower: bool = True,
    # tolerancia numérica
    atol: float = 0.0,
    rtol: float = 0.0,
) -> pd.DataFrame:
    """
    Compara df_left y df_right por 'keys' y devuelve:
      - keys (tal cual, una sola vez)
      - columnas seleccionadas de left con sufijo _{left_name}
      - columnas seleccionadas de right con sufijo _{right_name}
      - 'estado' y 'diferencias'

    Estados:
      - "OK"                       -> existe en ambos y no hay diferencias en compare_on
      - "DISCREPANCIA"             -> existe en ambos y hay diferencias
      - "Está en {left} pero no en {right}"
      - "Está en {right} pero no en {left}"
    """
    # Validar claves
    faltan_left = [k for k in keys if k not in df_left.columns]
    faltan_right = [k for k in keys if k not in df_right.columns]
    if faltan_left or faltan_right:
        raise ValueError(f"Claves ausentes. Left:{faltan_left}  Right:{faltan_right}")

    # Si no especifican columnas a incluir, tomamos todas las no-clave
    if include_left is None:
        include_left = [c for c in df_left.columns if c not in keys]
    else:
        include_left = [c for c in include_left if c in df_left.columns]

    if include_right is None:
        include_right = [c for c in df_right.columns if c not in keys]
    else:
        include_right = [c for c in include_right if c in df_right.columns]

    # Seleccionar y renombrar con sufijos
    left_sel = df_left[keys + list(include_left)].copy()
    right_sel = df_right[keys + list(include_right)].copy()

    left_ren = {c: f"{c}_{left_name}" for c in include_left}
    right_ren = {c: f"{c}_{right_name}" for c in include_right}

    left_sel = left_sel.rename(columns=left_ren)
    right_sel = right_sel.rename(columns=right_ren)

    # Outer merge para conservar todo
    merged = left_sel.merge(
        right_sel,
        on=keys,
        how="outer",
        indicator=True
    )

    # Determinar columnas base a comparar
    left_bases  = {c.rsplit(f"_{left_name}", 1)[0] for c in left_ren.values()}
    right_bases = {c.rsplit(f"_{right_name}", 1)[0] for c in right_ren.values()}
    if compare_on is None:
        compare_bases = sorted(list(left_bases & right_bases))
    else:
        compare_bases = [c for c in compare_on if (c in left_bases and c in right_bases)]

    # Normalización de texto previa (si procede) SOLO en las columnas incluidas
    if normalize_text_on:
        for base in normalize_text_on:
            col_l = f"{base}_{left_name}"
            col_r = f"{base}_{right_name}"
            if col_l in merged.columns:
                s = merged[col_l].astype("string")
                if strip: s = s.str.strip()
                if lower: s = s.str.lower()
                merged[col_l] = s
            if col_r in merged.columns:
                s = merged[col_r].astype("string")
                if strip: s = s.str.strip()
                if lower: s = s.str.lower()
                merged[col_r] = s

    # Helpers de comparación
    def iguales(v1, v2) -> bool:
        # NaN == NaN
        if pd.isna(v1) and pd.isna(v2):
            return True
        # Intentar numérico con tolerancia
        try:
            return np.isclose(float(v1), float(v2), atol=atol, rtol=rtol, equal_nan=True)
        except Exception:
            # Fallback: string comparado ya normalizado si tocaba
            return str(v1) == str(v2)

    # Construir estado + diferencias
    estados = []
    difs = []
    for _, row in merged.iterrows():
        where = row["_merge"]
        if where == "left_only":
            estados.append(f"Está en {left_name} pero no en {right_name}")
            difs.append([])
            continue
        if where == "right_only":
            estados.append(f"Está en {right_name} pero no en {left_name}")
            difs.append([])
            continue
        # En ambos: comparar compare_bases
        distintos = []
        for base in compare_bases:
            v1 = row.get(f"{base}_{left_name}", pd.NA)
            v2 = row.get(f"{base}_{right_name}", pd.NA)
            if not iguales(v1, v2):
                distintos.append(base)
        estados.append("OK" if not distintos else "DISCREPANCIA")
        difs.append(distintos)

    merged["estado"] = estados
    merged["diferencias"] = [", ".join(d) if d else "" for d in difs]

    # Orden de salida: keys + (left incluidas) + (right incluidas) + estado + diferencias
    out_cols = list(keys) + [f"{c}_{left_name}" for c in include_left] + [f"{c}_{right_name}" for c in include_right] + ["estado", "diferencias"]
    return merged[out_cols]
