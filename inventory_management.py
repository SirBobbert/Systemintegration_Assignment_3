# inventory_management.py
import json
import sqlite3
from flask import Flask, jsonify, g
from kafka import KafkaConsumer
import mysql.connector

app = Flask(__name__)
inventory_consumer = KafkaConsumer('purchase_history', bootstrap_servers='localhost:9092', group_id='inventory_group')

# MySQL database for inventory
MYSQL_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345678",
    "database": "inventory_management_db",
}

def get_db():
    if 'db' not in g:
        g.db = mysql.connector.connect(**MYSQL_DB_CONFIG)
    return g.db

def get_cursor():
    db = get_db()
    if 'cursor' not in g:
        g.cursor = db.cursor()
    return g.cursor

with app.app_context():
    cursor_inventory = get_cursor()
    cursor_inventory.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item VARCHAR(255),
            quantity INT
        )
    ''')
    get_db().commit()

@app.route('/check-inventory', methods=['GET'])
def check_inventory():
    cursor_inventory = get_cursor()
    cursor_inventory.execute('SELECT item, quantity FROM inventory')
    inventory_data = {item: quantity for item, quantity in cursor_inventory.fetchall()}
    return jsonify(inventory_data)

def listen_for_purchase_updates():
    with app.app_context():
        for message in inventory_consumer:
            update_data = message.value
            cursor_inventory = get_cursor()
            for item in update_data:
                # Update inventory in the database
                cursor_inventory.execute('UPDATE inventory SET quantity = quantity - %s WHERE item = %s', (item['quantity'], item['item']))
                get_db().commit()

if __name__ == '__main__':
    import threading
    inventory_listener = threading.Thread(target=listen_for_purchase_updates)
    inventory_listener.start()
    app.run(port=5002)
