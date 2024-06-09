import psycopg2
import pandas as pd

# Database connection parameters
oltp_db_params = {
    'dbname': 'db_coursework',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

olap_db_params = {
    'dbname': 'olap_db',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# Connect to OLTP and OLAP databases
oltp_conn = psycopg2.connect(**oltp_db_params)
olap_conn = psycopg2.connect(**olap_db_params)

oltp_cur = oltp_conn.cursor()
olap_cur = olap_conn.cursor()

# Step 1: Extract Data from OLTP
def extract_data():
    query = """
    SELECT o.id AS order_id, o.customer_id, o.date AS order_date, o.amount,
           od.product_id, od.quantity, od.unit_price,
           c.first_name, c.last_name, c.birthdate, c.email, c.phone,
           p.name AS product_name, p.category_id, p.supplier_id
    FROM orders o
    JOIN orderdetail od ON o.id = od.order_id
    JOIN customer c ON o.customer_id = c.id
    JOIN product p ON od.product_id = p.id;
    """
    oltp_cur.execute(query)
    data = oltp_cur.fetchall()
    columns = [desc[0] for desc in oltp_cur.description]
    return pd.DataFrame(data, columns=columns)

# Step 2: Transform Data
def transform_data(df):
    # Example transformation: calculating total sales amount
    df['total_sales'] = df['quantity'] * df['unit_price']
    return df

# Load Dimension Tables
def load_dim_tables():
    # Load Dim_Customer
    oltp_cur.execute("""
    SELECT c.id, c.first_name, c.last_name, c.email, a.address, '1900-01-01' AS start_date, NULL AS end_date, TRUE AS is_current
    FROM customer c
    JOIN address a ON c.address_id = a.id;
    """)
    customers = oltp_cur.fetchall()
    for customer in customers:
        olap_cur.execute("""
        INSERT INTO Dim_Customer (CustomerID, FirstName, LastName, Email, Address, StartDate, EndDate, IsCurrent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (CustomerID) DO NOTHING;
        """, customer)
    
    # Load Dim_Product
    oltp_cur.execute("SELECT id, name, category_id, price FROM product;")
    products = oltp_cur.fetchall()
    for product in products:
        olap_cur.execute("""
        INSERT INTO Dim_Product (ProductID, Name, Category, Price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ProductID) DO NOTHING;
        """, product)
    
    # Load Dim_Time
    oltp_cur.execute("SELECT DISTINCT date FROM orders;")
    dates = oltp_cur.fetchall()
    for date in dates:
        olap_cur.execute("""
        INSERT INTO Dim_Time (Date, Month, Quarter, Year)
        VALUES (%s, EXTRACT(MONTH FROM %s), EXTRACT(QUARTER FROM %s), EXTRACT(YEAR FROM %s))
        ON CONFLICT (Date) DO NOTHING;
        """, (date[0], date[0], date[0], date[0]))

    # Load Dim_Order
    oltp_cur.execute("SELECT id, customer_id, date, amount FROM orders;")
    orders = oltp_cur.fetchall()
    for order in orders:
        olap_cur.execute("""
        INSERT INTO Dim_Order (OrderID, CustomerID, OrderDate, TotalAmount)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (OrderID) DO NOTHING;
        """, order)

# Step 3: Load Data into OLAP
def load_fact_sales(df):
    for _, row in df.iterrows():
        olap_cur.execute("""
        INSERT INTO Fact_Sales (OrderID, CustomerID, Date, ProductID, Quantity_Sold, Total_Sales)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (OrderID, ProductID) DO NOTHING;
        """, (row['order_id'], row['customer_id'], row['order_date'], row['product_id'], row['quantity'], row['total_sales']))

def load_fact_payment():
    # Load Fact_Payment data from the orders table
    oltp_cur.execute("SELECT id, customer_id, date, amount FROM orders;")
    payments = oltp_cur.fetchall()
    for payment in payments:
        olap_cur.execute("""
        INSERT INTO Fact_Payment (PaymentID, OrderID, CustomerID, Date, Amount)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (PaymentID) DO NOTHING;
        """, (payment[0], payment[0], payment[1], payment[2], payment[3]))

# Run ETL process
def etl_process():
    df = extract_data()
    transformed_df = transform_data(df)
    load_dim_tables()
    load_fact_sales(transformed_df)
    load_fact_payment()
    oltp_conn.commit()
    olap_conn.commit()

# Execute the ETL process
etl_process()

# Close connections
oltp_cur.close()
olap_cur.close()
oltp_conn.close()
olap_conn.close()
