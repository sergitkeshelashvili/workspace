# performance_test.py
from timer import measure_endpoint_time, test_logger


def run_performance_checks():
    """Runs tests against all five API endpoints."""
    test_logger.info("--- Starting Performance Test (Target: < 1000ms) ---")

    # 1. Row Counts
    measure_endpoint_time("/api/v1/row_counts")

    # 2. Avg Sales
    measure_endpoint_time("/api/v1/avg_sales")

    # 3. Top Products (Using limit=10)
    measure_endpoint_time("/api/v1/top_products/10")

    # 4. Shipment Stats
    measure_endpoint_time("/api/v1/shipment_stats")

    # 5. Orders by Country
    measure_endpoint_time("/api/v1/orders_by_country")

    test_logger.info("--- Performance Test Complete ---")


if __name__ == "__main__":
    run_performance_checks()
