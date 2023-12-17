# test.py

import time
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
import joblib
import pandas as pd
import json

# Kafka konfigürasyonları
topic = 'iris'
bootstrap_servers = 'localhost:9092'

# Modeli yükleyen fonksiyon
def load_model():
    return joblib.load('iris_model.joblib')  # Modeli uygun bir şekilde yükleyin

# Veriyi modele veren ve tahmin yapan fonksiyon
def predict_data(model, data):
    # Sadece sayısal özellikleri al
    numerical_features = data[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
    
    # Tahmin yap
    prediction = model.predict(numerical_features)
    return prediction

# Dosyadaki verileri satır satır okuyan ve başa dönen fonksiyon
def read_csv_iter(file_path):
    while True:
        for chunk in pd.read_csv(file_path, chunksize=1):
            yield chunk

# Kafka'ya veri gönderen fonksiyon
def send_data_to_kafka(producer, data):
    # Veriyi JSON formatına çevir
    json_data = json.dumps(data.to_dict(orient='records'))
    
    # JSON veriyi bytes türüne çevir
    value_bytes = bytes(json_data, 'utf-8')
    
    producer.send(topic, value=value_bytes)

# Ana işlemi gerçekleştiren fonksiyon
def main():
    # Kafka producer konfigürasyonu
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    
    # Modeli yükle
    model = load_model()
    
    # Test verilerini satır satır okuyan iterator
    test_data_iter = read_csv_iter('test.csv')
    
    # Ana döngü
    while True:
        # Bir sonraki test verisini al
        test_data = next(test_data_iter)
        
        # Veriyi modele ver
        prediction = predict_data(model, test_data)
        print(f'Model Prediction: {prediction}')
        
        # Kafka'ya veriyi gönder
        send_data_to_kafka(producer, test_data)
        
        # 1 saniye bekle
        time.sleep(1)

if __name__ == '__main__':
    main()
