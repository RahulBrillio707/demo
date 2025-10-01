import pandas as pd

# Define items
items = ["Big Mac", "Fries", "Chicken Burger", "Veg Burger", "Ice Cream", "Muffin"]

# Define months
months = [
    "January", "February", "March", "April", "May",
    "June", "July", "August", "September", "October",
    "November", "December"
]

# Define seasonal multipliers
seasonal_multipliers = {
    "January": 1.0,
    "February": 1.0,
    "March": 1.2,
    "April": 1.2,
    "May": 1.2,
    "June": 1.5,
    "July": 1.4,
    "August": 1.2,
    "September": 1.2,
    "October": 1.2,
    "November": 1.3,
    "December": 1.4,
}

# Base monthly sales
base_sales = {
    "Big Mac": 1000,
    "Fries": 1200,
    "Chicken Burger": 900,
    "Veg Burger": 800,
    "Ice Cream": 700,
    "Muffin": 600
}

# Generate sales data from 2021–2024
years = [2021, 2022, 2023, 2024]
data = []

for year in years:
    growth_factor = 1 + 0.03 * (year - 2021)
    for month in months:
        for item in items:
            summer_multiplier = 1.2 if item in ["Ice Cream", "Big Mac", "Chicken Burger"] and month == "June" else 1.0
            sales = int(base_sales[item] * seasonal_multipliers[month] * growth_factor * summer_multiplier)
            data.append([year, month, item, sales])

# Create DataFrame
df = pd.DataFrame(data, columns=["Year", "Month", "Item", "Units Sold"])

# Pivot and save to Excel
pivot_df = df.pivot_table(index=["Year", "Month"], columns="Item", values="Units Sold").reset_index()
pivot_df.to_excel("Sales_Data_2021_2024.xlsx", index=False)

# print(f"Excel file 'Sales_Data ​:contentReferen
print("✅ Excel file 'Sales_Data_2021_2024.xlsx' has been created.")
