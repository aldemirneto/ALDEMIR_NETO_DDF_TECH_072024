import datetime
import great_expectations as gx
import json


# Function to append data to a JSON file
def append_to_json_file(file_path, new_data):
    try:
        # Read the existing data from the file
        try:
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []
        except json.JSONDecodeError:
            existing_data = []

        # Ensure the existing data is a list
        if not isinstance(existing_data, list):
            raise ValueError("The existing data in the file is not a list.")

        # Append the new data
        existing_data.append(new_data)

        # Write the updated data back to the file
        with open(file_path, 'w') as file:
            json.dump(existing_data, file, indent=4)

        print("Data successfully appended to the JSON file.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Path to the JSON file
file_path = 'data.json'

# Read the Parquet file
df = gx.read_parquet('2\denormalized_olist_dataset.parquet')


column_expectations = [
    df.expect_column_values_to_not_be_null('order_id', result_format="SUMMARY"),
    df.expect_column_values_to_be_unique('order_id', result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('order_status', ['delivered', 'shipped', 'processing', 'canceled'], result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_purchase_timestamp', "%Y-%m-%d %H:%M:%S", result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_approved_at', "%Y-%m-%d %H:%M:%S", result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_delivered_carrier_date', "%Y-%m-%d %H:%M:%S", result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_delivered_customer_date', "%Y-%m-%d %H:%M:%S", result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_estimated_delivery_date', "%Y-%m-%d %H:%M:%S", result_format="SUMMARY"),
    df.expect_column_values_to_be_between('order_item_id', 1, 10000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('price', 0, 10000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('freight_value', 0, 1000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_name_lenght', 1, 1000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_description_lenght', 1, 10000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_photos_qty', 0, 100, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_weight_g', 0, 100000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_length_cm', 0, 1000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_height_cm', 0, 1000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('product_width_cm', 0, 1000, result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('seller_state', ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'ES', 'PE', 'CE', 'PA', 'MT', 'GO', 'AM', 'RN', 'PB', 'DF', 'MS', 'MA', 'PI', 'SE', 'AL', 'RO', 'TO', 'AC', 'RR', 'AP'], result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('payment_type', ['credit_card', 'boleto', 'voucher', 'debit_card'], result_format="SUMMARY"),
    df.expect_column_values_to_be_between('payment_installments', 1, 24, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('payment_value', 0, 10000, result_format="SUMMARY"),
    df.expect_column_values_to_be_between('review_score', 1, 5, result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('order_id', 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', result_format="SUMMARY"),
    df.expect_column_values_to_match_strftime_format('customer_id', 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', result_format="SUMMARY"),
    df.expect_column_values_to_be_between('order_purchase_timestamp', min_value='1900-01-01 00:00:00', max_value=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('product_category_name', ['Eletrônicos', 'Roupas', 'Casa', 'Beleza'], result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('customer_city', ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Salvador'], result_format="SUMMARY"),
    df.expect_column_values_to_be_unique('review_id', result_format="SUMMARY"),
    df.expect_column_values_to_be_in_set('review_score', [1.0, 2.0, 3.0, 4.0, 5.0], result_format="SUMMARY"),
    df.expect_column_values_to_be_of_type('review_comment_title', 'str', result_format="SUMMARY"),
    df.expect_column_values_to_be_of_type('review_comment_message', 'str', result_format="SUMMARY"),
    df.expect_column_values_to_be_between('review_creation_date', min_value='1900-01-01', max_value=datetime.datetime.now().strftime("%Y-%m-%d"), result_format="SUMMARY")



]

# Column-level expectations

for result in column_expectations:
    x = {"success":result["success"], "test": result.expectation_config.expectation_type, "kwargs":result.expectation_config.kwargs
    ,"result":result["result"]}
    # Convert the dictionary to a JSON string
    x_json = json.dumps(x)
    # Load the JSON string to ensure it's correctly formatted
    x_loaded = json.loads(x_json)
    append_to_json_file(file_path, x)

print("All expectations have been processed and appended to the JSON file.")


