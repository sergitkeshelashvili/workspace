CREATE DATABASE food_delivery;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_uuid VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100),
    item_name VARCHAR(100),
    quantity INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM orders;
