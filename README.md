
# Initialize a dictionary to store the DataFrames
dataframes = {}

# Iterate over the child elements of the root
for table in root:
    # Get the table name from the attribute
    table_name = table.attrib.get('BUILD_STAND', 'BUILD_STAND')

    # Initialize a list to store the rows of the table
    rows = []

    # Iterate over the rows of the table
    for row in table:
        # Initialize a dictionary to store the row data
        row_data = {}

        # Iterate over the fields of the row
        for i, field in enumerate(row, start=1):
            # Get the field value
            field_value = field.text

            # Store the field data with a generic column name
            row_data[f'field{i}'] = field_value

        # Add the row data to the list of rows
        rows.append(row_data)

    # Create a DataFrame for the table and store it in the dictionary
    dataframes[table_name] = pd.DataFrame(rows)

# Access the DataFrames using the table names
df1 = dataframes.get('BUILD_STAND_TYPE')  
df2 = dataframes.get('BUILD_STAND') 
df3 = dataframes.get('BUILD_STAND_PRTN')  

# Print the DataFrames to verify
print(df1)
print(df2)
print(df3)
