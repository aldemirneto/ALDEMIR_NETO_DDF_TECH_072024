import pandas as pd

# Function to read CSV files
def read_csv(file_name):
    """
    Reads a CSV file and returns a DataFrame.

    Parameters:
    file_name (str): The name of the CSV file (without extension) to be read.

    Returns:
    pd.DataFrame: DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(f"{file_name}.csv")

# Read all CSV files
orders = read_csv("olist_orders_dataset")
order_items = read_csv("olist_order_items_dataset")
products = read_csv("olist_products_dataset")
sellers = read_csv("olist_sellers_dataset")
customers = read_csv("olist_customers_dataset")
payments = read_csv("olist_order_payments_dataset")
reviews = read_csv("olist_order_reviews_dataset")
geolocation = read_csv("olist_geolocation_dataset")

# Start with the orders dataset and merge other datasets
merged_df = orders.merge(order_items, on="order_id", how="left")
merged_df = merged_df.merge(products, on="product_id", how="left")
merged_df = merged_df.merge(sellers, on="seller_id", how="left")
merged_df = merged_df.merge(customers, on="customer_id", how="left")
merged_df = merged_df.merge(payments, on="order_id", how="left")
merged_df = merged_df.merge(reviews, on="order_id", how="left")

# Display the first few rows of the merged dataset
x = merged_df.head()

# Save the denormalized dataset
x.to_parquet("denormalized_olist_dataset.parquet", index=None)

# Print completion message
print("Denormalization complete. Output saved as 'denormalized_olist_dataset.parquet'")
