import json
from confluent_kafka import Consumer
import psycopg2

# --- 1. PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "food_delivery",
    "user": "postgres",
    "password": "123999",
    "port": "5532"
}

# --- Kafka
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])
print("🟢 Consumer is running and subscribed to orders topic")


try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    print("✅ PostgreSQL connection established.")
except Exception as e:
    print(f"❌ Failed to connect to PostgreSQL: {e}")
    exit()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():

            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)



        # 1. SQL
        insert_query = """
                       INSERT INTO orders (order_uuid, username, item_name, quantity)
                       VALUES (%s, %s, %s, %s) ON CONFLICT (order_uuid) DO NOTHING; \
                       """

        data = (
            order["order_id"],
            order["user"],
            order["item"],
            order["quantity"]
        )

        try:
            cursor.execute(insert_query, data)
            print(f"📦 Order {order['order_id']} successfully saved to DB.")
        except Exception as db_error:
            print(f"❌ DB Write Error for {order['order_id']}: {db_error}")

except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()
        print("✅ PostgreSQL connection closed.")
