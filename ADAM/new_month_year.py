import pandas as pd

# Read the CSV file
file_path = "FAST_FOOD_MENU_DATA.CSV"  # Replace with your file path
data = pd.read_csv(file_path)

# Create a list to store the new rows
new_rows = []

# Loop through the years and months
for year in range(2010, 2026):  # From 2010 to 2025
    for month in range(1, 13):  # From January (1) to December (12)
        temp_data = data.copy()  # Copy the original data
        temp_data['month'] = month
        temp_data['year'] = year
        new_rows.append(temp_data)  # Append the modified data to the list

# Concatenate all the new rows into a single DataFrame
expanded_data = pd.concat(new_rows, ignore_index=True)

# Select 3 items to analyze (replace with actual item names from your dataset)
items_to_analyze = ['Original Chicken Whopper Burger Family Size', 'Original Chicken Whopper Burger Extra Crispy', 'Crispy Chicken Wrap Family Size'] # Replace with actual item names

# Filter the data for the selected items
filtered_data = expanded_data[expanded_data['menu_item'].isin(items_to_analyze)]

# Get the count of occurrences for each item
item_counts = filtered_data['menu_item'].value_counts()

# Print the stats
print("Item Counts:")
print(item_counts)
# Save the expanded data to a new CSV file
output_file = "FAST_FOOD_MENU_DATA_EXPANDED_16_years.CSV"
expanded_data.to_csv(output_file, index=False)

print(f"Data expanded with months and years and saved to {output_file}")