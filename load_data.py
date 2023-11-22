import pandas as pd
import mysql.connector

# Load the dataset
data = pd.read_csv('/Users/chanderpal/Downloads/database.csv')

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='test',
    password='new_password',
    database='test'
)

# Create a cursor object
cursor = conn.cursor()

# Map Pandas data types to MySQL data types
dtype_mapping = {
    'object': 'VARCHAR(255)',
    'float64': 'FLOAT',
    'int64': 'INT',
}

# Replace 'nan' with None in the DataFrame
data = data.where(pd.notna(data), None)

# Extract column names and MySQL data types from the dataset
columns_and_types = [(col.replace(" ", "_"), dtype_mapping[str(data[col].dtype)]) for col in data.columns]

# Create neic_earthquakes table dynamically
create_table_query = f'''
CREATE TABLE neic_earthquakes (
    {", ".join([f"`{col}` {dtype}" for col, dtype in columns_and_types])}
);
'''
cursor.execute(create_table_query)

# Insert data into the table
for _, row in data.iterrows():
    # Replace 'nan' with None in the row
    row = [value if pd.notna(value) else None for value in row]

    insert_query = f'''
    INSERT INTO neic_earthquakes VALUES ({", ".join(["%s"] * len(row))});
    '''
    cursor.execute(insert_query, tuple(row))

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

