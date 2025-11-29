import psycopg2
from pgbenchmark import Benchmark

try:
    with psycopg2.connect(
            dbname="analytics_db",
            user="postgres",
            password="55555",
            host="localhost",
            port="5434"
    ) as conn:

        benchmark = Benchmark(db_connection=conn, number_of_runs=1000)


        sql_query = """
                SELECT c.country, COUNT(o.order_id) AS total_orders
                FROM dim_orders o 
                 JOIN 
                 dim_customers c ON o.customer_id = c.customer_id
                GROUP BY c.country
                ORDER BY total_orders DESC;  \
                    """
        benchmark.set_sql(sql_query)

        print("🚀 Starting benchmark...")
        for result in benchmark:
            pass

        print("---")
        print("✅ View Summary")
        print(benchmark.get_execution_results())



except psycopg2.OperationalError as e:
    print(f"❌ Database Connection Error: {e}")
