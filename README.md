
dataframes = {}

for table in root:

    table_name = table.attrib.get('name')

    # -----------------------------
    # 1️⃣ Obtener nombres de columnas
    # -----------------------------
    columns = []
    columns_node = table.find('Columns')

    if columns_node is not None:
        for col in columns_node:
            col_name = col.attrib.get('name')
            columns.append(col_name)

    # -----------------------------
    # 2️⃣ Leer SOLO los Row (datos)
    # -----------------------------
    rows = []

    for row in table.findall('Row'):   # 👈 CLAVE: solo Row
        row_data = {}

        for i, field in enumerate(row):
            value = field.text.strip() if field.text else None

            # usar nombres reales de columnas
            if i < len(columns):
                row_data[columns[i]] = value
            else:
                row_data[field.tag] = value

        rows.append(row_data)

    df = pd.DataFrame(rows)

    dataframes[table_name] = df


df1 = dataframes.get('tabla1')
df2 = dataframes.get('tabla2')
df3 = dataframes.get('tabla3')

print(df1.head())
print(df2.head())
print(df3.head())
