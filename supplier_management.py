# supplier_management.py

import mysql.connector
from flask import Flask, request, jsonify, g
from kafka import KafkaProducer
import json

app = Flask(__name__)

SUPPLIER_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345678",
    "database": "supplier_management_db",
}

supplier_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = mysql.connector.connect(**SUPPLIER_DB_CONFIG)
    return db

@app.route('/purchase-history', methods=['POST'])
def purchase_history():
    data = request.get_json()

    # Send the message to Kafka
    supplier_producer.send('purchase_history', value=data)

    # Save purchase history to the database
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('INSERT INTO purchase_history (item, quantity) VALUES (%s, %s)', (data['item'], data['quantity']))
        db.commit()


    return jsonify({"message": "Purchase history updated successfully!"})

if __name__ == '__main__':
    app.run(port=5001)
