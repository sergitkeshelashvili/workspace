-- Products
CREATE INDEX idx_fact_oi_product_qty ON fact_order_items (product_id, quantity DESC);

CREATE INDEX idx_dim_p_name ON dim_products (product_name);

-- Orders by Country

CREATE INDEX idx_dim_o_customer_id ON dim_orders (customer_id);
CREATE INDEX idx_dim_c_country ON dim_customers (country);

-- Shipment Stats
CREATE INDEX idx_fact_s_cost_ontime ON fact_shipments (shipping_cost, on_time_delivery);

