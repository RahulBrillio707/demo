import pandas as pd

# Read the CSV file
file_path = "FAST_FOOD_MENU_DATA.CSV"  # Replace with your file path
data = pd.read_csv(file_path)

# Function to generate unique code by taking the first letter of each word
def generate_unique_code(value):
    if pd.isna(value):  # Handle NaN values
        return ""
    return "".join(word[0] for word in str(value).split() if word)

# Function to generate class item code (first two letters)
def generate_class_item_code(value):
    if pd.isna(value):  # Handle NaN values
        return ""
    return str(value)[:2].upper()  # Take the first two letters and convert to uppercase

# Generate unique codes for each column
data['menu_item_code'] = data['menu_item'].apply(generate_unique_code)
data['category_item_code'] = data['category_item'].apply(generate_unique_code)
data['class_item_code'] = data['class_item'].apply(generate_class_item_code)

# Concatenate all three codes to generate the final unique code
data['final_unique_code'] = data['menu_item_code'] + data['category_item_code'] + data['class_item_code']

# Save the updated data to a new CSV file
output_file = "FAST_FOOD_MENU_DATA_WITH_FINAL_CODES2.CSV"
data.to_csv(output_file, index=False)

print(f"Unique codes generated and saved to {output_file}")