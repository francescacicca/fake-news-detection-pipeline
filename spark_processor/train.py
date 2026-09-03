import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, coalesce, lit, when, lower, trim
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


spark = SparkSession.builder \
        .appName("TrainFakeNewsDetectionModel") \
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Loading Dataset 
df = spark.read.csv("/app/data/news_dataset.csv", header=True, inferSchema=True)


# Pre-processing text from Dataset
df = df.withColumn(
    "clean_label",
    when(lower(trim(col("label").cast("string"))).isin("1", "1.0", "fake", "true"), 1.0)
    .when(lower(trim(col("label").cast("string"))).isin("0", "0.0", "real", "false"), 0.0)
    .otherwise(None)
)

df = df.withColumn(
    "full_text",
    concat_ws(" ", coalesce(col("title"), lit("")), coalesce(col("text"), lit("")))
)


df = df.filter(col("clean_label").isNotNull())
df = df.filter(trim(col("full_text")) != "")

# Splitting data: 80% for training, 20% for testing
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)


# Defining NLP Pipeline 
tokenizer = Tokenizer(inputCol="full_text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="clean_label", maxIter=20)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])


print("Training model on training set...")
model = pipeline.fit(train_df)


# Evaluation using Test Set
print("Training process completed. Evaluating model...")

predictions = model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(labelCol="clean_label", predictionCol="prediction")

accuracy = float(evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"}))
f1 = float(evaluator.evaluate(predictions, {evaluator.metricName: "f1"}))
precision = float(evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"}))
recall = float(evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"}))


print("\n" + "=" * 45)
print("          MODEL PERFORMANCE METRICS           ")
print("=" * 45)
print(f"Accuracy:           {accuracy * 100:.2f}%")
print(f"Weighted F1-Score:  {f1:.4f}")
print(f"Weighted Precision: {precision:.4f}")
print(f"Weighted Recall:    {recall:.4f}")
print("=" * 45 + "\n")


# Saving

MODEL_PATH = "/app/model/spark_fake_news_detection_pipeline"
model.write().overwrite().save(MODEL_PATH)
print(f"Completed. Model available @ path: {MODEL_PATH}")
spark.stop()