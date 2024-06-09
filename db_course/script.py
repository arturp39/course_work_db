import psycopg2
import pandas as pd
import random
from datetime import datetime, timedelta

# Load CSV files into DataFrames
customers_df = pd.read_csv('Customers.csv')
tech_accessories_df = pd.read_csv('TechAccessoriesData.csv')
orders_df = pd.read_csv('Orders.csv')  # Load the Orders CSV file

# Database connection parameters
db_params = {
    'dbname': 'db_coursework',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Extract unique countries and cities
unique_countries = customers_df['country'].unique()
unique_cities = customers_df[['city', 'country']].drop_duplicates()
unique_addresses = customers_df[['address', 'city', 'postal']].drop_duplicates()

# Load countries into the Country table and get their IDs
country_id_map = {}
for country in unique_countries:
    cur.execute("INSERT INTO country (country) VALUES (%s) ON CONFLICT (country) DO NOTHING RETURNING id;", (country,))
    result = cur.fetchone()
    if result is None:
        cur.execute("SELECT id FROM country WHERE country = %s;", (country,))
        result = cur.fetchone()
    country_id_map[country] = result[0]

# Load cities into the City table and get their IDs
city_id_map = {}
for _, row in unique_cities.iterrows():
    city, country = row['city'], row['country']
    country_id = country_id_map[country]
    cur.execute("INSERT INTO city (city, country_id) VALUES (%s, %s) ON CONFLICT (city) DO NOTHING RETURNING id;", (city, country_id))
    result = cur.fetchone()
    if result is None:
        cur.execute("SELECT id FROM city WHERE city = %s;", (city,))
        result = cur.fetchone()
    city_id_map[(city, country)] = result[0]

# Load addresses into the Address table and get their IDs
address_id_map = {}
for _, row in unique_addresses.iterrows():
    address, city, postal_code = row['address'], row['city'], row['postal']
    city_id = city_id_map[(city, customers_df[customers_df['address'] == address]['country'].values[0])]
    cur.execute("INSERT INTO address (address, city_id, postal_code) VALUES (%s, %s, %s) ON CONFLICT (address, city_id) DO NOTHING RETURNING id;", (address, city_id, postal_code))
    result = cur.fetchone()
    if result is None:
        cur.execute("SELECT id FROM address WHERE address = %s AND city_id = %s;", (address, city_id))
        result = cur.fetchone()
    address_id_map[(address, city)] = result[0]

# Update customers_df to use country, city, and address IDs
customers_df['country_id'] = customers_df['country'].map(country_id_map)
customers_df['city_id'] = customers_df.apply(lambda row: city_id_map[(row['city'], row['country'])], axis=1)
customers_df['address_id'] = customers_df.apply(lambda row: address_id_map[(row['address'], row['city'])], axis=1)

# Rename columns to match the database schema
customers_df.rename(columns={
    'firstname': 'first_name',
    'lastname': 'last_name',
    'createdate': 'create_date'
}, inplace=True)

# Prepare customer_columns with necessary adjustments
customer_columns = ['id', 'first_name', 'last_name', 'birthdate', 'email', 'phone', 'create_date', 'address_id']

# Load customer data into the Customer table
def load_data(df, table_name, columns):
    for i, row in df.iterrows():
        col_str = ', '.join(columns)
        val_str = ', '.join(['%s' for _ in columns])
        sql = f'INSERT INTO {table_name} ({col_str}) VALUES ({val_str});'
        cur.execute(sql, tuple(row[columns]))

load_data(customers_df, 'customer', customer_columns)

# Extract unique suppliers and categories
unique_suppliers = tech_accessories_df['supplier'].unique()
unique_categories = tech_accessories_df[['category', 'category_name', 'category_description']].drop_duplicates()

# Load suppliers into the Supplier table and get their IDs
supplier_id_map = {}
for supplier in unique_suppliers:
    cur.execute("INSERT INTO supplier (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (supplier,))
    result = cur.fetchone()
    if result is None:
        cur.execute("SELECT id FROM supplier WHERE name = %s;", (supplier,))
        result = cur.fetchone()
    supplier_id_map[supplier] = result[0]

# Load categories into the Category table and get their IDs
category_id_map = {}
for _, row in unique_categories.iterrows():
    category, name, description = row['category'], row['category_name'], row['category_description']
    cur.execute("INSERT INTO category (id, name, description) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING RETURNING id;", (category, name, description))
    result = cur.fetchone()
    if result is None:
        cur.execute("SELECT id FROM category WHERE id = %s;", (category,))
        result = cur.fetchone()
    category_id_map[category] = result[0]

# Update tech_accessories_df to use supplier and category IDs
tech_accessories_df['supplier_id'] = tech_accessories_df['supplier'].map(supplier_id_map)
tech_accessories_df['category_id'] = tech_accessories_df['category'].map(category_id_map)

# Rename columns to match the database schema
tech_accessories_df.rename(columns={
    'productId': 'id',
    'product': 'name'
}, inplace=True)

# Prepare product_columns with necessary adjustments
product_columns = ['id', 'name', 'description', 'price', 'quantity', 'category_id', 'supplier_id']

# Load product data into the Product table
load_data(tech_accessories_df, 'product', product_columns)

# Process Orders and OrderDetails

# Separate orders and order details
orders = orders_df[['OrderID', 'CustomerID', 'OrderDate', 'TotalAmount']].drop_duplicates().copy()
order_details = orders_df[['OrderID', 'ProductID', 'Quantity', 'TotalAmount']].copy()

# Rename columns to match the database schema
orders.rename(columns={
    'OrderID': 'id',
    'CustomerID': 'customer_id',
    'OrderDate': 'date',  # Column in the database is 'date'
    'TotalAmount': 'amount'  # Column in the database is 'amount'
}, inplace=True)

order_details.rename(columns={
    'OrderID': 'order_id',
    'ProductID': 'product_id',
    'Quantity': 'quantity',
    'TotalAmount': 'unit_price'  # Assuming TotalAmount is unit price
}, inplace=True)

# Load orders data into the Orders table
order_columns = ['id', 'customer_id', 'date', 'amount']
load_data(orders, 'orders', order_columns)

# Load order details data into the OrderDetail table
order_detail_columns = ['order_id', 'product_id', 'quantity', 'unit_price']
load_data(order_details, 'orderdetail', order_detail_columns)

conn.commit()
cur.close()
conn.close()
