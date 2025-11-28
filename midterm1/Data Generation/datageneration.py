import psycopg2
import random
import string
import csv
from datetime import datetime, timedelta
from io import StringIO

# -----------------------------
# CONFIG
# -----------------------------
CONN_PARAMS = {
    "dbname": "analytics_db", 
    "user": "postgres",
    "password": "55555",
    "host": "localhost",
    "port": 5532
}

# Target Data Volume
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 500
NUM_ORDERS = 500000  # Half a million order headers
NUM_ORDER_ITEMS = 1500000  # 1.5 million line items (for > 1M requirement)

# For reproducibility
random.seed(42)

# -----------------------------
# RANDOM GENERATOR FUNCTIONS
# -----------------------------
CITIES = ["New York", "London", "Paris", "Tokyo", "Berlin", "Sydney", "Dubai", "Singapore"]
COUNTRIES = ["USA", "GBR", "FRA", "JPN", "DEU", "AUS", "ARE", "SGP"]
PRODUCT_CATEGORIES = ["Electronics", "Apparel", "Home Goods", "Books", "Software", "Food"]
PAYMENT_METHODS = ["Credit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]
FIRST_NAMES = ["Anna", "John", "Maria", "Elias", "Sara", "Liam", "Noah", "Emma", "Oliver", "Sophia"]
LAST_NAMES = ["Smith", "Miller", "Brown", "Williams", "Johnson", "Davis", "Garcia", "Martinez"]


def random_name(names):
    return random.choice(names)


def random_email(first, last):
    domain = random.choice(["gmail.com", "outlook.com", "yahoo.com"])
    return f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{domain}"


def random_phone():
    return f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}"


def random_past_date(days=365):
    start = datetime.now() - timedelta(days=days)
    end = datetime.now()
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


# -----------------------------
# POPULATION FUNCTIONS
# -----------------------------

def populate_dim_customers(cur, count):
    print(f"Generating and inserting {count:,} customers...")
    customer_data = []
    # To reduce the chance of duplicates in the main data set, we use a set for tracking
    generated_emails = set()

    for i in range(count):
        fn = random_name(FIRST_NAMES)
        ln = random_name(LAST_NAMES)
        email = random_email(fn, ln)

        # A quick way to ensure we generate unique emails before insertion
        while email in generated_emails:
            email = random_email(fn, ln)
        generated_emails.add(email)

        country = random.choice(COUNTRIES)

        customer_data.append((
            f"{fn} {ln}",
            email,
            random_phone(),
            country,
            random_past_date(days=1000)
        ))

    # Use executemany for faster insertion
    # Removed 'gold.' prefix, targets 'public.dim_customers'
    cur.executemany(
        """
        INSERT INTO dim_customers (customer_name, email, phone, country, created_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING
        """,
        customer_data
    )


def populate_dim_products(cur, count):
    print(f"Generating and inserting {count:,} products...")
    product_data = []
    for i in range(count):
        product_data.append((
            f"Product {i + 1} - {random.choice(CITIES)}",
            random.choice(PRODUCT_CATEGORIES),
            round(random.uniform(5.00, 1500.00), 2),
            random_past_date(days=500)
        ))

    # Removed 'gold.' prefix, targets 'public.dim_products'
    cur.executemany(
        """
        INSERT INTO dim_products (product_name, category, unit_price, created_at)
        VALUES (%s, %s, %s, %s)
        """,
        product_data
    )


def populate_dim_orders(cur, count, customer_ids):
    print(f"Generating and inserting {count:,} orders...")
    order_data = []
    for i in range(count):
        order_date = random_past_date(days=300).date()
        order_data.append((
            random.choice(customer_ids),
            random.choice(PAYMENT_METHODS),
            order_date,
            # total_sales will be calculated by the fact table later, inserting 0 for now
            0.00
        ))

    # Removed 'gold.' prefix, targets 'public.dim_orders'
    cur.executemany(
        """
        INSERT INTO dim_orders (customer_id, payment_method, order_date, total_sales)
        VALUES (%s, %s, %s, %s)
        """,
        order_data
    )


def populate_fact_tables(cur, num_items, order_ids, product_ids):
    """Generates 1.5 million fact_order_items using COPY_FROM for high performance."""
    print(f"Generating and inserting {num_items:,} order items (1M+ requirement)...")

    # Use StringIO as a temporary file to hold data for COPY FROM
    output = StringIO()
    writer = csv.writer(output, delimiter='\t', lineterminator='\n')

    for i in range(num_items):
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        unit_price_paid = round(random.uniform(5.00, 1500.00), 2)
        line_item_sales = round(quantity * unit_price_paid * random.uniform(0.9, 1.1), 2)

        writer.writerow([
            order_id,
            product_id,
            quantity,
            unit_price_paid,
            line_item_sales,
            datetime.now()
        ])

    output.seek(0)

    # Copy the data into fact_order_items (fastest method)
    # Removed 'gold.' prefix, targets 'public.fact_order_items'
    cur.copy_from(
        output,
        'fact_order_items',
        columns=('order_id', 'product_id', 'quantity', 'unit_price_paid', 'line_item_sales', 'created_at')
    )

    # Populate fact_shipments with a fraction of the order items
    print("Generating and inserting shipments for a sample of order items...")

    # Removed 'gold.' prefix
    cur.execute(f"SELECT order_item_id FROM fact_order_items ORDER BY random() LIMIT {num_items // 3}")
    order_item_ids = [row[0] for row in cur.fetchall()]

    shipment_output = StringIO()
    shipment_writer = csv.writer(shipment_output, delimiter='\t', lineterminator='\n')

    for item_id in order_item_ids:
        cost = round(random.uniform(5.00, 50.00), 2)
        pickup = random_past_date(days=90).date()
        delivery = pickup + timedelta(days=random.randint(1, 7))
        on_time = random.choice([True] * 9 + [False] * 1)  # 90% on time
        damage = random.choice([False] * 99 + [True] * 1)  # 1% damage

        shipment_writer.writerow([
            item_id,
            cost,
            pickup,
            delivery,
            on_time,
            damage,
            datetime.now()
        ])

    shipment_output.seek(0)

    # Removed 'gold.' prefix, targets 'public.fact_shipments'
    cur.copy_from(
        shipment_output,
        'fact_shipments',
        columns=('order_item_id', 'shipping_cost', 'pickup_date', 'delivery_date', 'on_time_delivery',
                 'damage_reported', 'created_at')
    )


# -----------------------------
# MAIN EXECUTION
# -----------------------------
def run_analytics_populator():
    print("--- Starting Analytics Database Populator ---")
    try:
        with psycopg2.connect(**CONN_PARAMS) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # 1. Populate Dimension Tables
                populate_dim_customers(cur, NUM_CUSTOMERS)
                populate_dim_products(cur, NUM_PRODUCTS)

                # Commit dimensions to get their IDs
                conn.commit()
                print("Dimensions committed.")

                # 2. Fetch IDs for Foreign Keys
                # Removed 'gold.' prefix
                cur.execute("SELECT customer_id FROM dim_customers")
                customer_ids = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT product_id FROM dim_products")
                product_ids = [row[0] for row in cur.fetchall()]

                # 3. Populate dim_orders
                populate_dim_orders(cur, NUM_ORDERS, customer_ids)

                # Commit orders to get their IDs
                conn.commit()
                print("Orders committed.")

                # 4. Fetch Order IDs for Fact Table Mapping
                # Removed 'gold.' prefix
                cur.execute("SELECT order_id FROM dim_orders")
                order_ids = [row[0] for row in cur.fetchall()]

                # 5. Populate Fact Tables (The 1.5 Million Rows)
                populate_fact_tables(cur, NUM_ORDER_ITEMS, order_ids, product_ids)

                conn.commit()
                print("Fact tables committed.")

                print("\n--- Verification ---")
                # Removed 'gold.' prefix
                cur.execute("SELECT count(*) FROM fact_order_items")
                print(f"Total rows in fact_order_items: {cur.fetchone()[0]:,}")
                cur.execute("SELECT count(*) FROM fact_shipments")
                print(f"Total rows in fact_shipments: {cur.fetchone()[0]:,}")

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        print("Rollback performed. Check your database connection and schema.")

    print("\nDone! Massive data generation complete 👍")


if __name__ == "__main__":
    run_analytics_populator()
