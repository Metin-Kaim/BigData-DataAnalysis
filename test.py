import findspark
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, FloatType
import pandas as pd
# PYSPARK_PYTHON değişkenini belirle
findspark.init()

# Spark Session oluştur
spark = SparkSession.builder.master("local").appName("IrisTest").getOrCreate()

# Eğitilmiş modeli yükle
model = PipelineModel.load("iris_model_spark")

# Test verisi şeması
schema = StructType([
    StructField("sepal_length", FloatType(), True),
    StructField("sepal_width", FloatType(), True),
    StructField("petal_length", FloatType(), True),
    StructField("petal_width", FloatType(), True)
])

# Test verilerini oku ve Spark DataFrame'e dönüştür
def read_csv_iter(file_path, schema, chunksize=1):
    for chunk in pd.read_csv(file_path, chunksize=chunksize, dtype={'sepal_length': float, 'sepal_width': float, 'petal_length': float, 'petal_width': float}):
        spark_df = spark.createDataFrame(chunk, schema=schema)
        yield spark_df

def predict_data(model, data):
    prediction = model.transform(data)
    return prediction.select("sepal_length", "sepal_width", "petal_length", "petal_width", "prediction")

def main():
    test_data_iter = read_csv_iter("test.csv", schema)

    while True:
        try:
            # Bir sonraki test verisini al
            test_data = next(test_data_iter)

            # Veriyi modele ver ve çıktıyı al
            prediction = predict_data(model, test_data)

            # Çıktıyı konsola yazdır
            prediction.show()

        except StopIteration:
            # Dosyanın sonuna gelindiğinde dosyayı tekrar başa al
            test_data_iter = read_csv_iter("test.csv", schema)

if __name__ == "__main__":
    main()
