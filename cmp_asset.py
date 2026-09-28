import pandas as pd


def comparar_dataframes_completos(
    df1: pd.DataFrame,
    df2: pd.DataFrame
) -> pd.DataFrame:
    """
    Compara dos DataFrames por todas sus columnas.

    Devuelve un DataFrame con todos los registros de ambos y una
    columna RESULTADO indicando:

        IGUAL     -> el registro existe en ambos DataFrames
        SOLO_DF1  -> el registro solo existe en df1
        SOLO_DF2  -> el registro solo existe en df2
    """

    # Comprobamos que ambos tengan las mismas columnas
    if set(df1.columns) != set(df2.columns):
        raise ValueError(
            "Los DataFrames no tienen las mismas columnas.\n"
            f"Solo en df1: {set(df1.columns) - set(df2.columns)}\n"
            f"Solo en df2: {set(df2.columns) - set(df1.columns)}"
        )

    # Ponemos las columnas en el mismo orden
    df2 = df2[df1.columns]

    resultado = df1.merge(
        df2,
        how="outer",
        on=list(df1.columns),
        indicator=True
    )

    resultado["RESULTADO"] = resultado["_merge"].map({
        "both": "IGUAL",
        "left_only": "SOLO_DF1",
        "right_only": "SOLO_DF2"
    })

    resultado = resultado.drop(columns="_merge")

    return resultado
	
def comparar_assets(
    df_asset1,
    df_tci1,
    df_partitions1,
    df_asset2,
    df_tci2,
    df_partitions2
):
    """
    Compara dos Assets previamente transformados a DataFrames.

    Returns
    -------
    result_asset
    result_tci
    result_partitions
    """

    result_asset = comparar_dataframes_completos(
        df_asset1,
        df_asset2
    )

    result_tci = comparar_dataframes_completos(
        df_tci1,
        df_tci2
    )

    result_partitions = comparar_dataframes_completos(
        df_partitions1,
        df_partitions2
    )

    return (
        result_asset,
        result_tci,
        result_partitions
    )

#llamamos
from common.dataframe_utils import comparar_dataframes_completos
df_asset1, df_tci1, df_partitions1 = transform_asset(
    workbook_asset1
)

df_asset2, df_tci2, df_partitions2 = transform_asset(
    workbook_asset2
)
result_asset, result_tci, result_partitions = comparar_assets(
    df_asset1,
    df_tci1,
    df_partitions1,
    df_asset2,
    df_tci2,
    df_partitions2
)
