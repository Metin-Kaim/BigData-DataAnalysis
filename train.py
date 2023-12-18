from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import joblib

# Spark Session oluştur
spark = SparkSession.builder.appName("IrisClassifier").getOrCreate()

# Veriyi oku (train.csv dosyasını uygun şekilde değiştirin)
iris_data = spark.read.csv('train.csv', header=True, inferSchema=True)

# StringIndexer ile etiketleri sayısal türlere dönüştür
label_indexer = StringIndexer(inputCol="species", outputCol="indexed_species_temp")
iris_data = label_indexer.fit(iris_data).transform(iris_data)

# Önceki adımın ardından yeni bir adım ekleyerek sütun adını değiştir
iris_data = iris_data.withColumnRenamed("indexed_species_temp", "indexed_species")

# Özellikler ve etiketleri bir araya getir
feature_cols = iris_data.columns[:-2]  # "species" ve "indexed_species" sütunlarını çıkart
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
classifier = RandomForestClassifier(labelCol="indexed_species", featuresCol="features", numTrees=100, seed=42)
pipeline = Pipeline(stages=[label_indexer, vector_assembler, classifier])

# Veriyi eğit
model = pipeline.fit(iris_data)

# Modeli kaydet
model.write().overwrite().save("iris_model_spark")

# Spark Session'ı kapat
spark.stop()
