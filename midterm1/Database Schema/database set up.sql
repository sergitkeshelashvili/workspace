-- 1. Database Creation (Must be run while connected to a different database, like 'postgres')
CREATE DATABASE analytics_db;

-- 2. Connect to the new 'analytics_db' database to run the following scripts.

-- DIMENSION 1: Customers
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(50) NOT NULL,
    email VARCHAR(50) UNIQUE,
    phone VARCHAR(50),
    country CHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIMENSION 2: Products
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIMENSION 3: Orders
CREATE TABLE dim_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES dim_customers(customer_id),
    payment_method VARCHAR(30),
    order_date DATE NOT NULL,
    total_sales DECIMAL(12,2)
);

-- FACT 1: Order Line Items (The Core Sales Event - Target for 1M+ rows)
CREATE TABLE fact_order_items (
    order_item_id BIGSERIAL PRIMARY KEY,

    order_id INTEGER NOT NULL REFERENCES dim_orders(order_id),
    product_id INTEGER NOT NULL REFERENCES dim_products(product_id),

    -- Metrics
    quantity INTEGER NOT NULL,
    unit_price_paid DECIMAL(10,2) NOT NULL,
    line_item_sales DECIMAL(12,2) NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FACT 2: Shipments (The Logistical Event)
CREATE TABLE fact_shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    order_item_id BIGINT NOT NULL REFERENCES fact_order_items(order_item_id),
    shipping_cost DECIMAL(10,2),
    pickup_date DATE,
    delivery_date DATE,
    on_time_delivery BOOLEAN DEFAULT TRUE,
    damage_reported BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
