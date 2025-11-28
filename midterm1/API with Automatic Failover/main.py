from fastapi import FastAPI, HTTPException
from db_manager import get_db_results, OperationalError
import uvicorn # 

# Create the FastAPI application instance
app = FastAPI(
    title="PostgreSQL Failover Read API",
    description="Reads data from Slave by default, failing over to Master when necessary."
)

# --- ENDPOINTS ---

@app.get("/api/v1/row_counts")
def get_all_row_counts():
    """Endpoint 1: Row counts for dim_customers and fact_order_items."""
    query = """
            SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count 
            FROM dim_customers
            UNION ALL
            SELECT 'fact_order_items' AS table_name, COUNT(*) AS row_count 
            FROM fact_order_items; 
            """
    try:
        results, server = get_db_results(query)
        return {
            "source": server,
            "description": "Row Counts for Key Tables",
            "results": results
        }
    except OperationalError as e:
        # Handle database unavailability with a 503 Service Unavailable error
        raise HTTPException(status_code=503, detail=f"Database cluster unavailable: {e}")


@app.get("/api/v1/avg_sales")
def get_avg_sales():
    """Endpoint 2: Aggregation - Average Line Item Sales."""
    query = "SELECT AVG(line_item_sales) AS average_line_item_sales FROM fact_order_items;"

    try:
        results, server = get_db_results(query)
        return {
            "source": server,
            "description": "Average Line Item Sales",
            "results": results[0]
        }
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database cluster unavailable: {e}")


@app.get("/api/v1/top_products/{limit}")
def get_top_products(limit: int):
    """Endpoint 3: Top-N results - Top selling products by quantity."""
    query = f"""
        SELECT 
            p.product_name, 
            SUM(i.quantity) AS total_quantity_sold
        FROM 
            dim_products p
        JOIN 
            fact_order_items i ON p.product_id = i.product_id
        GROUP BY 
            p.product_name
        ORDER BY 
            total_quantity_sold DESC
        LIMIT {limit};
    """
    try:
        results, server = get_db_results(query)
        return {
            "source": server,
            "description": f"Top {limit} Selling Products",
            "results": results
        }
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database cluster unavailable: {e}")


@app.get("/api/v1/shipment_stats")
def get_shipment_stats():
    """Endpoint 4: Aggregation - Min/Max/Avg Shipping Cost and On-Time Rate."""
    query = """
            SELECT MIN(shipping_cost) AS min_cost, 
                   MAX(shipping_cost) AS max_cost, 
                   AVG(shipping_cost) AS avg_cost, 
                   CAST(SUM(CASE WHEN on_time_delivery = TRUE THEN 1 ELSE 0 END) AS NUMERIC) * 100 / 
                   COUNT(*) AS on_time_rate_percent
            FROM fact_shipments; 
            """
    try:
        results, server = get_db_results(query)
        return {
            "source": server,
            "description": "Shipment Cost and On-Time Rate Statistics",
            "results": results[0]
        }
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database cluster unavailable: {e}")


@app.get("/api/v1/orders_by_country")
def get_orders_by_country():
    """Endpoint 5: Joins & Filtered Queries - Total orders per country."""
    query = """
            SELECT c.country, 
                   COUNT(o.order_id) AS total_orders
            FROM dim_orders o 
                 JOIN 
                 dim_customers c ON o.customer_id = c.customer_id
            GROUP BY c.country
            ORDER BY total_orders DESC; 
            """
    try:
        results, server = get_db_results(query)
        return {
            "source": server,
            "description": "Total Orders Grouped by Customer Country",
            "results": results
        }
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database cluster unavailable: {e}")

# ---------------------------------------------
# EXECUTION BLOCK for PyCharm/Direct Running
# ---------------------------------------------
if __name__ == "__main__":
    # It starts the Uvicorn server on host 0.0.0.0 and port 8000.
    uvicorn.run(app, host="0.0.0.0", port=8000)
