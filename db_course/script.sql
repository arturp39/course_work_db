-- Drop existing tables if they exist
DROP TABLE IF EXISTS CartItem CASCADE;
DROP TABLE IF EXISTS Cart CASCADE;
DROP TABLE IF EXISTS Review CASCADE;
DROP TABLE IF EXISTS Inventory CASCADE;
DROP TABLE IF EXISTS OrderDetail CASCADE;
DROP TABLE IF EXISTS Orders CASCADE;
DROP TABLE IF EXISTS Product CASCADE;
DROP TABLE IF EXISTS Supplier CASCADE;
DROP TABLE IF EXISTS Category CASCADE;
DROP TABLE IF EXISTS Customer CASCADE;
DROP TABLE IF EXISTS Address CASCADE;
DROP TABLE IF EXISTS City CASCADE;
DROP TABLE IF EXISTS Country CASCADE;

-- Create tables in the correct order
CREATE TABLE country(
    id bigserial NOT NULL,
    country TEXT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(country)
);

CREATE TABLE city(
    id bigserial NOT NULL,
    city TEXT NOT NULL,
    country_id BIGINT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(city),
    FOREIGN KEY (country_id) REFERENCES country (id)
);

CREATE TABLE address (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    city_id BIGINT NOT NULL,
    postal_code TEXT NOT NULL,
    CONSTRAINT address_city_unique UNIQUE (address, city_id),
    FOREIGN KEY (city_id) REFERENCES city (id)
);


CREATE TABLE customer(
    id bigserial NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    birthdate DATE NOT NULL,
    email TEXT NOT NULL,
    phone TEXT NOT NULL,
    address_id BIGINT NOT NULL,
    create_date DATE NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(email),
    UNIQUE(phone),
    FOREIGN KEY (address_id) REFERENCES address (id)
);

-- Create indexes on Customer table
CREATE INDEX idx_customer_email ON customer (email);
CREATE INDEX idx_customer_phone ON customer (phone);

CREATE TABLE category(
    id bigserial NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(name)
);

-- Create indexes on Category table
CREATE INDEX idx_category_name ON category (name);

CREATE TABLE supplier(
    id bigserial NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(name)
);

-- Create indexes on Supplier table
CREATE INDEX idx_supplier_name ON supplier (name);

CREATE TABLE product(
    id bigserial NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    supplier_id BIGINT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(name),
    FOREIGN KEY (category_id) REFERENCES category (id),
    FOREIGN KEY (supplier_id) REFERENCES supplier (id)
);

-- Create indexes on Product table
CREATE INDEX idx_product_name ON product (name);
CREATE INDEX idx_product_category_id ON product (category_id);
CREATE INDEX idx_product_supplier_id ON product (supplier_id);

CREATE TABLE orders(
    id bigserial NOT NULL,
    customer_id BIGINT NOT NULL,
    date DATE NOT NULL,
    amount BIGINT NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY (customer_id) REFERENCES customer (id)
);

-- Create indexes on Orders table
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_date ON orders (date);

CREATE TABLE orderdetail(
    id bigserial NOT NULL,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price BIGINT NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (product_id) REFERENCES product (id)
);

-- Create indexes on OrderDetail table
CREATE INDEX idx_orderdetail_order_id ON orderdetail (order_id);
CREATE INDEX idx_orderdetail_product_id ON orderdetail (product_id);

CREATE TABLE inventory(
    id bigserial NOT NULL,
    product_id BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(product_id),
    FOREIGN KEY (product_id) REFERENCES product (id)
);

-- Create indexes on Inventory table
CREATE INDEX idx_inventory_product_id ON inventory (product_id);

CREATE TABLE review(
    id bigserial NOT NULL,
    product_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    rating INTEGER NOT NULL,
    comment TEXT NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY (product_id) REFERENCES product (id),
    FOREIGN KEY (customer_id) REFERENCES customer (id)
);

-- Create indexes on Review table
CREATE INDEX idx_review_product_id ON review (product_id);
CREATE INDEX idx_review_customer_id ON review (customer_id);

CREATE TABLE cart(
    id bigserial NOT NULL,
    customer_id BIGINT NOT NULL,
    creation_date DATE NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY (customer_id) REFERENCES customer (id)
);

-- Create indexes on Cart table
CREATE INDEX idx_cart_customer_id ON cart (customer_id);

CREATE TABLE cartitem(
    id bigserial NOT NULL,
    cart_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    price BIGINT NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY (cart_id) REFERENCES cart (id),
    FOREIGN KEY (product_id) REFERENCES product (id)
);

-- Create indexes on CartItem table
CREATE INDEX idx_cartitem_cart_id ON cartitem (cart_id);
CREATE INDEX idx_cartitem_product_id ON cartitem (product_id);

-- Create Roles and Assign Privileges
CREATE ROLE admin;
CREATE ROLE regular_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO regular_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;

-- Test queries
SELECT * FROM orders;
SELECT * FROM customer;
select * from product;


