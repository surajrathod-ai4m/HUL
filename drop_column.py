import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('/home/suraj-rathod/Desktop/HUL/CSVs/laminate_jamming_20240704152409.csv')

# Display the columns before dropping
print("Columns before dropping:")
print(df.columns)

# Remove the specified column
column_to_remove = 'detection'  # Replace 'column_name' with the name of the column you want to remove
df.drop(column_to_remove, axis=1, inplace=True)

# Display the columns after dropping
print("Columns after dropping:")
print(df.columns)

# Save the updated DataFrame back to a new CSV file
df.to_csv('/home/suraj-rathod/Desktop/HUL/CSVs/main_laminate_jamming_20240704152409.csv', index=False)
