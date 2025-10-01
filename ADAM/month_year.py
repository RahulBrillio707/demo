import pandas as pd

# Read the CSV file
file_path = "FAST_FOOD_MENU_DATA.CSV"  # Replace with your file path
data = pd.read_csv(file_path)

# Add the first month and year columns
data['month'] = 1
data['year'] = 2010

# Add the second month and year columns
data['month_2'] = 2
data['year_2'] = 2010

# Save the updated data to a new CSV file
output_file = "FAST_FOOD_MENU_DATA_month_year.CSV"
data.to_csv(output_file, index=False)

print(f"Month and year columns added and saved to {output_file}")