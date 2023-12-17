from kafka import KafkaProducer
import json
import time

# Kafka yapılandırması
topic = 'iris'
bootstrap_servers = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
from kafka import KafkaProducer
import time

# Kafka yapılandırması
topic = 'iris'
bootstrap_servers = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: str(v).encode('utf-8'))  # str olarak encode et

# Test verileri yolu
test_data_path = "test.csv"

# Test verilerini Kafka'ya gönder
with open(test_data_path, 'r') as file:
    lines = file.readlines()
    for line in lines:
        producer.send(topic, value=line.strip())  # JSON dönüşümü olmadan direkt gönder
        time.sleep(1)  # 1 saniye bekle

producer.close()

# Test verileri yolu
test_data_path = "test.csv"

# Test verilerini Kafka'ya gönder
with open(test_data_path, 'r') as file:
    lines = file.readlines()
    for line in lines:
        data = json.loads(line)
        producer.send(topic, value=data)
        time.sleep(1)  # 1 saniye bekle

producer.close()
