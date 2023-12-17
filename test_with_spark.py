from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import findspark
findspark.init()

# Spark Session oluştur
spark = SparkSession.builder.appName("IrisModelTesting").getOrCreate()

# Kafka yapılandırması
topic = 'iris'
bootstrap_servers = 'localhost:9092'

# Veri şemasını tanımla
schema = StructType([StructField("sepal_length", DoubleType(), True),
                     StructField("sepal_width", DoubleType(), True),
                     StructField("petal_length", DoubleType(), True),
                     StructField("petal_width", DoubleType(), True)])

# Modeli yükle
model = PipelineModel.load("iris_model")

# Kafka üzerinden veri akışı al
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic)
    .load()
)

# JSON verilerini ayrıştır
parsed_data = (
    kafka_stream.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", schema).alias("data"))
    .select("data.*")
)

# Veri setini hazırla
vector_assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")
prepared_data = vector_assembler.transform(parsed_data)

# Modeli kullanarak tahmin yap
predictions = model.transform(prepared_data)

# Sonuçları konsol ekranına yazdır
query = (
    predictions.select("sepal_length", "sepal_width", "petal_length", "petal_width", "prediction")
    .writeStream.outputMode("append").format("console").start()
)

query.awaitTermination()
