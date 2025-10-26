from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import SerializingProducer
import json

app = Flask(__name__)
CORS(app)  # Enable CORS để frontend có thể gọi API

# Kafka configuration
producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092'
})

topic = 'financial_transactions'

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

@app.route('/transaction', methods=['POST'])
def create_transaction():
    try:
        # Nhận data từ frontend
        transaction = request.json
        
        print(f"📦 Received transaction: {transaction['transactionId']}")
        
        # Gửi lên Kafka
        producer.produce(
            topic,
            key=transaction['transactionId'],
            value=json.dumps(transaction),
            on_delivery=delivery_report
        )
        producer.poll(0)
        
        return jsonify({
            'status': 'success',
            'message': 'Transaction sent to Kafka',
            'transactionId': transaction['transactionId']
        }), 200
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'Server is running'}), 200

if __name__ == '__main__':
    print("🚀 Starting Flask server on http://localhost:5000")
    print("📡 Connected to Kafka at localhost:9092")
    app.run(debug=True, host='0.0.0.0', port=5000)