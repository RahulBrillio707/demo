import pandas as pd
import hashlib

# Read the CSV file
file_path = "FAST_FOOD_MENU_DATA.CSV"  # Replace with your file path
data = pd.read_csv(file_path)

# Function to generate unique codes using hashing
def generate_unique_code(value):
    return hashlib.md5(value.encode()).hexdigest()[:8]  # Generate an 8-character hash

# Generate unique codes for each column
data['menu_item_code'] = data['menu_item'].apply(lambda x: generate_unique_code(str(x)))
data['category_item_code'] = data['category_item'].apply(lambda x: generate_unique_code(str(x)))
data['class_item_code'] = data['class_item'].apply(lambda x: generate_unique_code(str(x)))

# Save the updated data to a new CSV file
output_file = "FAST_FOOD_MENU_DATA_WITH_CODES.CSV"
data.to_csv(output_file, index=False)

print(f"Unique codes generated and saved to {output_file}")