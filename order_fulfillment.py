# order_status_management.py
import json
from flask import Flask, jsonify
from kafka import KafkaConsumer
import mysql.connector
import threading

app = Flask(__name__)

# MySQL database for order status
MYSQL_ORDER_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345678",
    "database": "order_fulfillment_db",
}

conn_order_status = mysql.connector.connect(**MYSQL_ORDER_DB_CONFIG)
cursor_order_status = conn_order_status.cursor()
cursor_order_status.execute('''
    CREATE TABLE IF NOT EXISTS order_status (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(255),
        status VARCHAR(255)
    )
''')
conn_order_status.commit()

# Kafka consumer for purchase history
order_consumer = KafkaConsumer('purchase_history', bootstrap_servers='localhost:9092', group_id='order_group')

# Lock for synchronization
order_lock = threading.Lock()

# Flag to check if the thread has started
order_listener_started = False

def listen_for_purchase_updates():
    try:
        for message in order_consumer:
            update_data = message.value
            for item in update_data:
                # Acquire a lock to avoid conflicts with the global cursor
                with order_lock:
                    # Update order status in the database
                    cursor_order_status.execute('INSERT INTO order_status (order_id, status) VALUES (%s, %s) ON DUPLICATE KEY UPDATE status = %s',
                                                (item['order'], 'Shipped', 'Shipped'))
                    conn_order_status.commit()
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")

if not order_listener_started:
    order_listener_started = True
    order_listener = threading.Thread(target=listen_for_purchase_updates)
    order_listener.start()

def get_cursor():
    cursor = conn_order_status.cursor()
    return cursor

@app.route('/order-status', methods=['GET'])
def get_order_status():
    try:
        # Acquire a lock to avoid conflicts with the global cursor
        with order_lock:
            cursor_order_status_request = get_cursor()
            cursor_order_status_request.execute('SELECT order_id, status FROM order_status')
            order_status_data = {order_id: status for order_id, status in cursor_order_status_request.fetchall()}
            return jsonify(order_status_data)
    except Exception as e:
        print(f"Error in get_order_status: {e}")
    finally:
        cursor_order_status_request.close()  # Close the cursor

if __name__ == '__main__':
    app.run(port=5003)
