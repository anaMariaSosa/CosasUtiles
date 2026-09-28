# =========================
# SELECCIONAR COLUMNAS
# =========================

df_subset = df[["LCN", "PN", "SERIAL_NO"]]


# =========================
# FILTRAR FILAS
# =========================

df_filtered = df[df["LCN"] == "X8321"]

df_filtered = df[
    (df["LCN"] == "X8321") &
    (df["STATUS"] == "FI")
]

df_filtered = df[
    df["LCN"].isin(["X8321", "X8324", "X4911"])
]


# =========================
# ELIMINAR COLUMNAS
# =========================

df = df.drop(columns=["columna1", "columna2"])


# =========================
# ELIMINAR FILAS
# =========================

df = df.drop(index=[0, 1, 2])


# =========================
# ELIMINAR DUPLICADOS
# =========================

df = df.drop_duplicates()

df = df.drop_duplicates(
    subset=["LCN", "PN"]
)


# =========================
# RENOMBRAR COLUMNAS
# =========================

df = df.rename(columns={
    "PART_NUMBER": "PN",
    "SERIAL_NUMBER": "SN"
})


# =========================
# MERGE
# =========================

df_result = df_left.merge(
    df_right,
    on="LCN",
    how="left"
)


# Varias claves
df_result = df_left.merge(
    df_right,
    on=["LCN", "PN"],
    how="left"
)


# Columnas con nombres distintos
df_result = df_left.merge(
    df_right,
    left_on="LCN",
    right_on="LCN_CODE",
    how="left"
)


# =========================
# SABER DE DÓNDE VIENE
# =========================

df_result = df_left.merge(
    df_right,
    on="LCN",
    how="outer",
    indicator=True
)

# _merge:
# left_only
# right_only
# both


# =========================
# CONCATENAR DATAFRAMES
# =========================

df_total = pd.concat(
    [df_a, df_b],
    ignore_index=True
)


# =========================
# CREAR COLUMNA
# =========================

df["LCN_PN"] = (
    df["LCN"].astype(str)
    + "_"
    + df["PN"].astype(str)
)


# =========================
# VALORES ÚNICOS
# =========================

valores = df["LCN"].unique()

numero = df["LCN"].nunique()


# =========================
# CONTAR
# =========================

len(df)

df["LCN"].value_counts()


# =========================
# NULOS
# =========================

df[df["PN"].isna()]

df[df["PN"].notna()]

df["PN"] = df["PN"].fillna("VOID")


# =========================
# ORDENAR
# =========================

df = df.sort_values(
    by=["LCN", "PN"]
)


# =========================
# GROUP BY
# =========================

resultado = (
    df.groupby("LCN")
      .size()
      .reset_index(name="COUNT")
)


# =========================
# COPIA
# =========================

df_result = df[["LCN", "PN"]].copy()

# =========================
# comparar_por_clave()
# =========================
comparison = df_a.merge(
    df_b,
    on=["LCN", "PN"],
    how="outer",
    indicator=True
)

only_a = comparison[
    comparison["_merge"] == "left_only"
]

only_b = comparison[
    comparison["_merge"] == "right_only"
]

both = comparison[
    comparison["_merge"] == "both"
]