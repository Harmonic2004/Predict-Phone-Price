#Before, install kafka-python library first
import csv
from kafka import KafkaProducer
import json

# Cấu hình Kafka
kafka_broker = 'localhost:9092'  # kafka broker location
topic = 'send_csv_to_kafka_topic'  # Tên của topic trong Kafka

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Đọc file CSV và gửi từng dòng vào Kafka
csv_file_path = '/home/h-user/Predict-Phone-Price/Data_collection/final_data.csv'  # Đường dẫn đến file CSV của bạn

with open(csv_file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        producer.send(topic, row)
        print(f"Sent: {row}")

# Đợi cho các tin nhắn được gửi đi trước khi kết thúc
producer.flush()

